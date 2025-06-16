import { connectToDatabase } from './mongodb'

const WEATHER_API_KEY = process.env.OPENWEATHER_API_KEY
const LAT = 47.06149235737582
const LON = 28.86683069635403

const WEATHER_ICONS = {
  '01d': '☀️', // clear sky
  '01n': '🌙', // clear sky night
  '02d': '⛅', // few clouds
  '02n': '☁️', // few clouds night
  '03d': '☁️', // scattered clouds
  '03n': '☁️', // scattered clouds night
  '04d': '☁️', // broken clouds
  '04n': '☁️', // broken clouds night
  '09d': '🌧️', // shower rain
  '09n': '🌧️', // shower rain night
  '10d': '🌦️', // rain
  '10n': '🌧️', // rain night
  '11d': '⛈️', // thunderstorm
  '11n': '⛈️', // thunderstorm night
  '13d': '🌨️', // snow
  '13n': '🌨️', // snow night
  '50d': '🌫️', // mist
  '50n': '🌫️', // mist night
}

export async function fetchWeatherData() {
  try {
    const response = await fetch(
      `https://api.openweathermap.org/data/2.5/forecast?lat=${LAT}&lon=${LON}&appid=${WEATHER_API_KEY}&units=metric`
    )
    const data = await response.json()
    
    const dailyForecasts = data.list.reduce((acc, item) => {
      const date = new Date(item.dt * 1000)
      const day = date.toISOString().split('T')[0]
      
      if (!acc[day]) {
        acc[day] = {
          date: day,
          temp_min: item.main.temp_min,
          temp_max: item.main.temp_max,
          humidity: item.main.humidity,
          wind_speed: item.wind.speed,
          icon: item.weather[0].icon,
          description: item.weather[0].description,
          timestamp: date
        }
      } else {
        acc[day].temp_min = Math.min(acc[day].temp_min, item.main.temp_min)
        acc[day].temp_max = Math.max(acc[day].temp_max, item.main.temp_max)
      }
      
      return acc
    }, {})

    return Object.values(dailyForecasts)
  } catch (error) {
    console.error('Error fetching weather data:', error)
    throw error
  }
}

export async function getWeatherData() {
  const { db } = await connectToDatabase()
  const collection = db.collection('weather_data')
  
  const latestData = await collection.findOne({}, { sort: { timestamp: -1 } })
  
  if (!latestData || (Date.now() - latestData.timestamp.getTime() > 2 * 60 * 60 * 1000)) {
    const newData = await fetchWeatherData()
    await collection.insertOne({
      forecasts: newData,
      timestamp: new Date()
    })
    return newData
  }
  
  return latestData.forecasts
}

export function getWeatherIcon(iconCode) {
  return WEATHER_ICONS[iconCode] || '❓'
} 