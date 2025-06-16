import { NextResponse } from 'next/server';
import { connectToDatabase } from '@/lib/mongodb';

const WEATHER_IMPACT = {
  CLOUD_IMPACT: {
    CLEAR: 1.0,
    PARTLY_CLOUDY: 0.75,
    MOSTLY_CLOUDY: 0.5,
    OVERCAST: 0.25,
  },
  TEMP_IMPACT: {
    OPTIMAL: 1.0,
    COLD: 0.9,
    HOT: 0.8,
  },
  RAIN_IMPACT: {
    NONE: 1.0,
    LIGHT: 0.7,
    HEAVY: 0.3,
  }
};

function getCloudImpact(description) {
  if (!description) return WEATHER_IMPACT.CLOUD_IMPACT.PARTLY_CLOUDY;

  const desc = description.toLowerCase();
  if (desc.includes('clear') || desc.includes('sunny')) return WEATHER_IMPACT.CLOUD_IMPACT.CLEAR;
  if (desc.includes('few clouds') || desc.includes('partly')) return WEATHER_IMPACT.CLOUD_IMPACT.PARTLY_CLOUDY;
  if (desc.includes('scattered') || desc.includes('mostly')) return WEATHER_IMPACT.CLOUD_IMPACT.MOSTLY_CLOUDY;
  if (desc.includes('overcast') || desc.includes('cloudy')) return WEATHER_IMPACT.CLOUD_IMPACT.OVERCAST;
  return WEATHER_IMPACT.CLOUD_IMPACT.PARTLY_CLOUDY;
}

function getTempImpact(temp) {
  if (typeof temp !== 'number' || isNaN(temp)) return WEATHER_IMPACT.TEMP_IMPACT.OPTIMAL;

  if (temp >= 20 && temp <= 25) return WEATHER_IMPACT.TEMP_IMPACT.OPTIMAL;
  if (temp < 10) return WEATHER_IMPACT.TEMP_IMPACT.COLD;
  if (temp > 35) return WEATHER_IMPACT.TEMP_IMPACT.HOT;

  if (temp < 20) {
    return 0.9 + ((temp - 10) / 10) * 0.1;
  } else {
    return 1.0 - ((temp - 25) / 10) * 0.2;
  }
}

function getRainImpact(description) {
  if (!description) return WEATHER_IMPACT.RAIN_IMPACT.NONE;

  const desc = description.toLowerCase();
  if (desc.includes('rain') || desc.includes('shower')) {
    if (desc.includes('heavy') || desc.includes('thunderstorm')) {
      return WEATHER_IMPACT.RAIN_IMPACT.HEAVY;
    }
    return WEATHER_IMPACT.RAIN_IMPACT.LIGHT;
  }
  if (desc.includes('snow') || desc.includes('thunderstorm')) {
    return WEATHER_IMPACT.RAIN_IMPACT.HEAVY;
  }
  return WEATHER_IMPACT.RAIN_IMPACT.NONE;
}

async function ensureWeatherData(db) {
  console.log('Checking for weather data...');

  let weatherData = await db.collection('weather_data')
      .findOne({}, { sort: { timestamp: -1 } });

  console.log('Weather data found:', !!weatherData);

  if (!weatherData || !weatherData.forecasts) {
    console.log('No weather data found, attempting to fetch...');

    try {
      const baseUrl = process.env.NEXTAUTH_URL || 'http://localhost:3000';
      const response = await fetch(`${baseUrl}/api/weather?refresh=true`, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      if (response.ok) {
        console.log('Weather API call successful, checking database again...');
        weatherData = await db.collection('weather_data')
            .findOne({}, { sort: { timestamp: -1 } });
      }
    } catch (error) {
      console.error('Failed to fetch weather data internally:', error);
    }
  }

  return weatherData;
}

async function calculateSimplePrediction(weatherData, historicalData) {
  console.log('Calculating predictions with weather data:', weatherData?.length, 'historical data:', historicalData?.length);

  let basePower = 800;

  if (historicalData && historicalData.length > 0) {
    const validData = historicalData.filter(d =>
        d && d.data && typeof d.data.pv_input_power === 'number' && d.data.pv_input_power > 0
    );

    if (validData.length > 0) {
      const avgPower = validData.reduce((sum, d) => sum + d.data.pv_input_power, 0) / validData.length;
      basePower = Math.max(200, avgPower);
      console.log('Calculated base power from historical data:', basePower);
    }
  }

  if (!weatherData || !Array.isArray(weatherData)) {
    console.error('Invalid weather data:', weatherData);
    throw new Error('Invalid weather data provided');
  }

  const predictions = weatherData.slice(0, 7).map((dayWeather, index) => {
    if (!dayWeather) {
      throw new Error(`Weather data missing for day ${index}`);
    }

    const avgTemp = dayWeather.temp_max && dayWeather.temp_min
        ? (dayWeather.temp_max + dayWeather.temp_min) / 2
        : 20; // Default temp

    const cloudImpact = getCloudImpact(dayWeather.description);
    const tempImpact = getTempImpact(avgTemp);
    const rainImpact = getRainImpact(dayWeather.description);

    const weatherFactor = cloudImpact * tempImpact * rainImpact;
    const predictedPower = Math.round(basePower * weatherFactor);

    const daysFuture = index + 1;
    const baseConfidence = 0.9 - (daysFuture * 0.1);
    const weatherConfidence = (cloudImpact + tempImpact + rainImpact) / 3;
    const confidence = Math.max(0.3, baseConfidence * weatherConfidence);

    return {
      date: dayWeather.date ? new Date(dayWeather.date) : new Date(Date.now() + index * 24 * 60 * 60 * 1000),
      predictedPower,
      weatherData: dayWeather,
      confidence: Math.round(confidence * 100) / 100,
      factors: {
        cloudImpact,
        tempImpact,
        rainImpact,
        weatherFactor
      }
    };
  });

  console.log('Generated predictions:', predictions.length);
  return predictions;
}

export async function GET(request) {
  console.log('GET /api/predictions called');

  try {
    const { db } = await connectToDatabase();
    console.log('Database connected successfully');

    const { searchParams } = new URL(request.url);
    const forceRefresh = searchParams.get('refresh') === 'true';
    console.log('Force refresh:', forceRefresh);

    if (!forceRefresh) {
      console.log('Checking for cached predictions...');
      const existingPredictions = await db.collection('predictions')
          .find({})
          .sort({ date: 1 })
          .toArray();

      console.log('Found cached predictions:', existingPredictions.length);

      if (existingPredictions.length > 0) {
        const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000);
        const latestPrediction = existingPredictions[0];

        if (latestPrediction.lastUpdated && new Date(latestPrediction.lastUpdated) > sixHoursAgo) {
          return NextResponse.json({
            predictions: existingPredictions,
            fromCache: true,
            lastUpdated: latestPrediction.lastUpdated
          });
        } else {
          console.log('Cached predictions are stale, will refresh');
        }
      }
    }

    const weatherData = await ensureWeatherData(db);

    if (!weatherData || !weatherData.forecasts) {
      console.error('No weather data available after attempting to fetch');
      return NextResponse.json(
          {
            error: 'No weather data available. Please check your weather API configuration.',
            suggestion: 'Ensure WEATHER_API_KEY is set and the weather API is accessible.'
          },
          { status: 400 }
      );
    }

    console.log('Weather data available with forecasts:', weatherData.forecasts.length);

    console.log('Fetching historical solar data...');
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    const historicalData = await db.collection('solar_data')
        .find({ timestamp: { $gte: thirtyDaysAgo } })
        .toArray();

    console.log('Historical data found:', historicalData.length, 'records');

    console.log('Calculating new predictions...');
    const predictions = await calculateSimplePrediction(weatherData.forecasts, historicalData);

    const now = new Date();
    const predictionsWithTimestamp = predictions.map(pred => ({
      ...pred,
      lastUpdated: now
    }));

    console.log('Storing predictions in database...');
    await db.collection('predictions').deleteMany({});
    if (predictionsWithTimestamp.length > 0) {
      await db.collection('predictions').insertMany(predictionsWithTimestamp);
    }

    console.log('Predictions stored successfully');

    return NextResponse.json({
      predictions: predictionsWithTimestamp,
      fromCache: false,
      lastUpdated: now
    });

  } catch (error) {
    console.error('Prediction API error:', error);
    return NextResponse.json(
        {
          error: error.message || 'Failed to generate predictions',
          stack: process.env.NODE_ENV === 'development' ? error.stack : undefined,
          timestamp: new Date().toISOString()
        },
        { status: 500 }
    );
  }
}

export async function POST(request) {
  console.log('POST /api/predictions called - forcing refresh');
  return GET(new Request(request.url + '?refresh=true'));
}