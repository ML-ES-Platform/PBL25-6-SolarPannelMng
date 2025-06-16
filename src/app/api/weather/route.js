import { NextResponse } from 'next/server'
import { connectToDatabase } from '@/lib/mongodb'

const CACHE_DURATION = 2 * 60 * 60 * 1000 // 2 hours in milliseconds
const COORDS = {
  lat: 47.06149235737582,
  lon: 28.86683069635403
}

async function fetchWeatherFromAPI() {
  console.log('Fetching weather from WeatherAPI...')

  if (!process.env.WEATHER_API_KEY) {
    throw new Error('WEATHER_API_KEY environment variable is not set')
  }

  const response = await fetch(
      `http://api.weatherapi.com/v1/forecast.json?key=${process.env.WEATHER_API_KEY}&q=${COORDS.lat},${COORDS.lon}&days=7&aqi=no`,
      {
        headers: {
          'User-Agent': 'SolarPrediction/1.0'
        }
      }
  )

  if (!response.ok) {
    const errorText = await response.text()
    console.error('WeatherAPI Error:', response.status, errorText)
    throw new Error(`Weather API failed: ${response.status} - ${errorText}`)
  }

  const data = await response.json()
  console.log('Weather data fetched successfully:', !!data.forecast?.forecastday)

  return data
}

function convertToInternalFormat(weatherApiData) {
  if (!weatherApiData.forecast?.forecastday) {
    throw new Error('Invalid weather data structure from WeatherAPI')
  }

  return weatherApiData.forecast.forecastday.map(day => ({
    date: day.date,
    temp_min: day.day.mintemp_c,
    temp_max: day.day.maxtemp_c,
    humidity: day.day.avghumidity,
    wind_speed: day.day.maxwind_kph / 3.6,
    icon: day.day.condition.icon,
    description: day.day.condition.text.toLowerCase(),
    timestamp: new Date(day.date)
  }))
}

export async function GET(request) {
  console.log('Weather API GET called')

  try {
    const { searchParams } = new URL(request.url)
    const forceRefresh = searchParams.get('refresh') === 'true'

    const { db } = await connectToDatabase()
    console.log('Database connected successfully')

    const cachedWeather = await db.collection('weather').findOne({
      'location.lat': COORDS.lat,
      'location.lon': COORDS.lon
    })

    const now = new Date()
    const isCacheValid = cachedWeather &&
        (now - new Date(cachedWeather.lastUpdated)) < CACHE_DURATION

    console.log('Cache status:', {
      hasCached: !!cachedWeather,
      isCacheValid,
      forceRefresh
    })

    if (isCacheValid && !forceRefresh) {
      console.log('Returning cached weather data')
      return NextResponse.json(cachedWeather)
    }

    console.log('Fetching fresh weather data...')
    const weatherData = await fetchWeatherFromAPI()

    const weatherWithMetadata = {
      ...weatherData,
      location: COORDS,
      lastUpdated: now
    }

    await db.collection('weather').updateOne(
        {
          'location.lat': COORDS.lat,
          'location.lon': COORDS.lon
        },
        { $set: weatherWithMetadata },
        { upsert: true }
    )

    const internalFormatData = convertToInternalFormat(weatherData)

    await db.collection('weather_data').updateOne(
        { type: 'forecast' },
        {
          $set: {
            forecasts: internalFormatData,
            timestamp: now,
            source: 'weatherapi'
          }
        },
        { upsert: true }
    )

    console.log('Weather data stored in both formats successfully')

    return NextResponse.json(weatherWithMetadata)

  } catch (error) {
    console.error('Weather API Error:', error)

    try {
      const { db } = await connectToDatabase()
      const cachedWeather = await db.collection('weather').findOne({
        'location.lat': COORDS.lat,
        'location.lon': COORDS.lon
      })

      if (cachedWeather) {
        console.log('Returning stale cached data due to API error')
        return NextResponse.json({
          ...cachedWeather,
          isStale: true,
          error: 'Using cached data due to API error'
        })
      }
    } catch (dbError) {
      console.error('Database error while fetching cached data:', dbError)
    }

    return NextResponse.json(
        {
          error: 'Failed to fetch weather data',
          details: error.message,
          timestamp: new Date().toISOString()
        },
        { status: 500 }
    )
  }
}