'use client'

import { useState, useEffect } from 'react'
import { Card } from '@/components/ui/card'
import { BatteryStatus } from '@/components/BatteryStatus'
import { PowerStats } from '@/components/PowerStats'
import { SystemStatus } from '@/components/SystemStatus'
import { SolarChart } from '@/components/SolarChart'
import { motion } from 'framer-motion'
import { Navbar } from '@/components/Navbar'
import WeatherForecast from '@/components/WeatherForecast'
import PowerPredictionGraph from '@/components/PowerPredictionGraph'

export default function Dashboard() {
  const [data, setData] = useState([])
  const [currentData, setCurrentData] = useState([])
  const [chartTimeRange, setChartTimeRange] = useState('today')
  const [loading, setLoading] = useState(true)
  const [minLoadingComplete, setMinLoadingComplete] = useState(false)
  const [lastUpdateTime, setLastUpdateTime] = useState(null)

  const fetchCurrentData = async () => {
    try {
      const url = lastUpdateTime 
        ? `/api/solar-data?timeRange=today&since=${lastUpdateTime}`
        : '/api/solar-data?timeRange=today'
      
      const response = await fetch(url)
      const newData = await response.json()
      
      if (newData.length > 0) {
        setCurrentData(prevData => {
          const mergedData = [...prevData]
          newData.forEach(newItem => {
            const newTimestamp = newItem.timestamp.$date || newItem.timestamp
            const existingIndex = mergedData.findIndex(item => 
              (item.timestamp.$date || item.timestamp) === newTimestamp
            )
            if (existingIndex === -1) {
              mergedData.push(newItem)
            } else {
              mergedData[existingIndex] = newItem
            }
          })
          return mergedData
        })

        const latestTimestamp = newData[newData.length - 1].timestamp.$date || newData[newData.length - 1].timestamp
        setLastUpdateTime(latestTimestamp)

        if (chartTimeRange === 'today') {
          setData(prevData => {
            const mergedData = [...prevData]
            newData.forEach(newItem => {
              const newTimestamp = newItem.timestamp.$date || newItem.timestamp
              const existingIndex = mergedData.findIndex(item => 
                (item.timestamp.$date || item.timestamp) === newTimestamp
              )
              if (existingIndex === -1) {
                mergedData.push(newItem)
              } else {
                mergedData[existingIndex] = newItem
              }
            })
            return mergedData
          })
        }
      }
    } catch (error) {
      console.error('Error fetching current data:', error)
    }
  }

  const fetchHistoricalData = async () => {
    try {
      setLastUpdateTime(null);
      const response = await fetch(`/api/solar-data?timeRange=${chartTimeRange}`)
      const newData = await response.json()
      setData(newData)
    } catch (error) {
      console.error('Error fetching historical data:', error)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    const minLoadingTimer = setTimeout(() => {
      setMinLoadingComplete(true)
    }, 500)

    fetchCurrentData()
    const currentInterval = setInterval(fetchCurrentData, 20000)
    
    return () => {
      clearInterval(currentInterval)
      clearTimeout(minLoadingTimer)
    }
  }, [])

  useEffect(() => {
    fetchHistoricalData()
  }, [chartTimeRange])

  if (loading || !minLoadingComplete) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900">
        <motion.div
          initial={{ scale: 0.5, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          transition={{ duration: 0.5 }}
          className="relative"
        >
          <div className="w-32 h-32 border-4 border-blue-500 border-t-transparent rounded-full animate-spin"></div>
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="w-24 h-24 border-4 border-emerald-500 border-t-transparent rounded-full animate-spin" style={{ animationDirection: 'reverse' }}></div>
          </div>
        </motion.div>
      </div>
    )
  }

  const latestData = currentData[currentData.length - 1]?.data || {}
  const lastUpdateTimeStr = currentData[currentData.length - 1]?.timestamp?.$date || currentData[currentData.length - 1]?.timestamp

  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.1
      }
    }
  }

  const itemVariants = {
    hidden: { y: 20, opacity: 0 },
    visible: {
      y: 0,
      opacity: 1,
      transition: {
        duration: 0.5
      }
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 text-white">
      <Navbar lastUpdateTime={lastUpdateTime} />
      <div className="max-w-[1400px] mx-auto p-4 sm:p-6 space-y-6">
        <motion.div 
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          className="w-full"
        >
          <WeatherForecast />
        </motion.div>

        <motion.div 
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6"
        >
          <motion.div variants={itemVariants} className="h-full">
            <BatteryStatus data={latestData} history={currentData} />
          </motion.div>
          <motion.div variants={itemVariants} className="h-full">
            <PowerStats data={latestData} />
          </motion.div>
          <motion.div variants={itemVariants} className="h-full">
            <SystemStatus data={latestData} history={currentData} />
          </motion.div>
        </motion.div>

        <motion.div 
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          className="w-full"
        >
          <PowerPredictionGraph />
        </motion.div>

        <motion.div 
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6"
        >
          <motion.div variants={itemVariants} className="h-full">
            <Card className="h-full p-4 sm:p-6 bg-gray-800/50 backdrop-blur-sm border-gray-700 hover:bg-gray-800/70 transition-all duration-300 transform hover:scale-[1.02] hover:shadow-2xl hover:shadow-purple-500/10">
              <SolarChart data={data} type="voltage" timeRange={chartTimeRange} onTimeRangeChange={setChartTimeRange} />
            </Card>
          </motion.div>
          <motion.div variants={itemVariants} className="h-full">
            <Card className="h-full p-4 sm:p-6 bg-gray-800/50 backdrop-blur-sm border-gray-700 hover:bg-gray-800/70 transition-all duration-300 transform hover:scale-[1.02] hover:shadow-2xl hover:shadow-blue-500/10">
              <SolarChart data={data} type="power" timeRange={chartTimeRange} onTimeRangeChange={setChartTimeRange} />
            </Card>
          </motion.div>
          <motion.div variants={itemVariants} className="h-full">
            <Card className="h-full p-4 sm:p-6 bg-gray-800/50 backdrop-blur-sm border-gray-700 hover:bg-gray-800/70 transition-all duration-300 transform hover:scale-[1.02] hover:shadow-2xl hover:shadow-amber-500/10">
              <SolarChart data={data} type="current" timeRange={chartTimeRange} onTimeRangeChange={setChartTimeRange} />
            </Card>
          </motion.div>
          <motion.div variants={itemVariants} className="h-full">
            <Card className="h-full p-4 sm:p-6 bg-gray-800/50 backdrop-blur-sm border-gray-700 hover:bg-gray-800/70 transition-all duration-300 transform hover:scale-[1.02] hover:shadow-2xl hover:shadow-red-500/10">
              <SolarChart data={data} type="temperature" timeRange={chartTimeRange} onTimeRangeChange={setChartTimeRange} />
            </Card>
          </motion.div>
        </motion.div>
      </div>
    </div>
  )
}

