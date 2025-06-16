'use client';

import { useEffect, useState } from 'react';
import Image from 'next/image';
import { Card } from '@/components/ui/card'
import { motion } from 'framer-motion'

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

export default function WeatherForecast() {
  const [weatherData, setWeatherData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchWeather = async () => {
      try {
        const response = await fetch('/api/weather');
        if (!response.ok) throw new Error('Failed to fetch weather data');
        const data = await response.json();
        setWeatherData(data);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchWeather();
  }, []);

  if (loading) {
    return (
      <Card className="w-full p-3 bg-gray-800/50 backdrop-blur-sm border-gray-700">
        <div className="flex items-center justify-center h-24">
          <motion.div
            initial={{ scale: 0.5, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            transition={{ duration: 0.5 }}
            className="relative"
          >
            <div className="w-16 h-16 border-4 border-blue-500 border-t-transparent rounded-full animate-spin"></div>
            <div className="absolute inset-0 flex items-center justify-center">
              <div className="w-12 h-12 border-4 border-emerald-500 border-t-transparent rounded-full animate-spin" style={{ animationDirection: 'reverse' }}></div>
            </div>
          </motion.div>
        </div>
      </Card>
    );
  }

  if (error) {
    return (
      <Card className="w-full p-3 bg-gray-800/50 backdrop-blur-sm border-gray-700">
        <div className="flex items-center justify-center h-24">
          <div className="text-red-500">Error loading weather data</div>
        </div>
      </Card>
    );
  }

  if (!weatherData?.forecast?.forecastday) {
    return null;
  }

  return (
    <Card className="w-full p-3 bg-gray-800/50 backdrop-blur-sm border-gray-700 hover:bg-gray-800/70 transition-all duration-300">
      <div className="flex justify-between items-center mb-2">
        <div>
          <h2 className="text-lg font-semibold text-white">7-Day Forecast</h2>
          <div className="text-xs text-gray-400">
            Chisinau, UTM
          </div>
        </div>
        <div className="text-xs text-gray-400">
          Last updated: {new Date(weatherData.lastUpdated).toLocaleTimeString()}
        </div>
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-7 gap-2">
        {weatherData.forecast.forecastday.map((day, index) => (
          <motion.div
            key={day.date}
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: index * 0.1 }}
            className="flex flex-col items-center p-2 rounded-lg bg-gray-700/30 hover:bg-gray-700/50 transition-all duration-300"
          >
            <div className="text-xs text-gray-400">
              {index === 0 ? 'Today' : new Date(day.date).toLocaleDateString('en-US', { weekday: 'short' })}
            </div>
            <div className="flex items-center gap-1 sm:gap-2">
              <div className="relative w-8 h-8 sm:w-10 sm:h-10">
                <Image
                  src={`https:${day.day.condition.icon}`}
                  alt={day.day.condition.text}
                  fill
                  className="object-contain"
                />
              </div>
              <div className="flex flex-col">
                <span className="text-sm font-semibold text-white">
                  {Math.round(day.day.maxtemp_c)}°
                </span>
                <span className="text-xs text-gray-400">
                  {Math.round(day.day.mintemp_c)}°
                </span>
              </div>
            </div>
            <div className="text-[10px] sm:text-xs text-gray-500 text-center mt-0.5 line-clamp-1">
              {day.day.condition.text}
            </div>
            <div className="text-[10px] sm:text-xs text-gray-400">
              {day.day.daily_chance_of_rain}%
            </div>
          </motion.div>
        ))}
      </div>
    </Card>
  )
} 