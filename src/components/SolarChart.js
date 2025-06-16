import { Card } from '@/components/ui/card'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'
import { useState } from 'react'

export function SolarChart({ data, type, timeRange, onTimeRangeChange }) {
  const getFilteredData = (data) => {
    const now = new Date()
    const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate())
    const startOfYesterday = new Date(startOfDay)
    startOfYesterday.setDate(startOfDay.getDate() - 1)
    const endOfYesterday = new Date(startOfYesterday)
    endOfYesterday.setHours(23, 59, 59, 999)
    const startOfWeek = new Date(startOfDay)
    startOfWeek.setDate(startOfDay.getDate() - 7)
    const startOfMonth = new Date(startOfDay)
    startOfMonth.setMonth(startOfDay.getMonth() - 1)

    let startTime, endTime
    switch (timeRange) {
      case 'today':
        startTime = startOfDay
        endTime = now
        break
      case 'yesterday':
        startTime = startOfYesterday
        endTime = endOfYesterday
        break
      case 'week':
        startTime = startOfWeek
        endTime = now
        break
      case 'month':
        startTime = startOfMonth
        endTime = now
        break
      default:
        startTime = startOfDay
        endTime = now
    }

    return data.filter(item => {
      const dateStr = item.timestamp.$date || item.timestamp
      const date = new Date(dateStr)
      return date >= startTime && date <= endTime
    })
  }

  const formatData = (data) => {
    return getFilteredData(data).map(item => {
      const dateStr = item.timestamp.$date || item.timestamp
      const date = new Date(dateStr)
      
      if (isNaN(date.getTime())) {
        console.error('Invalid date:', dateStr)
        return null
      }

      return {
        timestamp: date.toLocaleString('en-US', {
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          hour12: true
        }),
        batteryLevel: item.data.battery_capacity,
        batteryVoltage: item.data.battery_voltage,
        batteryVoltageFromScc: item.data.battery_voltage_from_scc,
        chargingCurrent: item.data.battery_charging_current,
        batteryDischargeCurrent: item.data.battery_discharge_current,
        pvInputPower: item.data.pv_input_power,
        acOutputPower: item.data.ac_output_active_power,
        acOutputApparentPower: item.data.ac_output_apparent_power,
        acOutputLoad: item.data.ac_output_load,
        inverterTemp: item.data.inverter_heat_sink_temperature,
        pvInputVoltage: item.data.pv_input_voltage,
        acInputVoltage: item.data.ac_input_voltage,
        acOutputVoltage: item.data.ac_output_voltage,
        busVoltage: item.data.bus_voltage
      }
    }).filter(Boolean) // Remove any null entries from invalid dates
  }

  const getChartConfig = () => {
    switch(type) {
      case 'power':
        return {
          lines: [
            { key: 'pvInputPower', name: 'PV Input Power (W)', color: '#10B981' },
            { key: 'acOutputPower', name: 'AC Output Power (W)', color: '#3B82F6' },
            { key: 'acOutputApparentPower', name: 'AC Apparent Power (VA)', color: '#F59E0B' },
            { key: 'acOutputLoad', name: 'AC Output Load (%)', color: '#EF4444' }
          ]
        }
      case 'battery':
        return {
          lines: [
            { key: 'batteryVoltage', name: 'Battery Voltage (V)', color: '#3B82F6' }
          ]
        }
      case 'current':
        return {
          lines: [
            { key: 'chargingCurrent', name: 'Charging Current (A)', color: '#F59E0B' },
            { key: 'batteryDischargeCurrent', name: 'Discharge Current (A)', color: '#EF4444' }
          ]
        }
      case 'voltage':
        return {
          lines: [
            { key: 'batteryVoltage', name: 'Battery Voltage (V)', color: '#3B82F6' },
            { key: 'pvInputVoltage', name: 'PV Input Voltage (V)', color: '#10B981' },
            { key: 'acInputVoltage', name: 'AC Input Voltage (V)', color: '#8B5CF6' },
            { key: 'acOutputVoltage', name: 'AC Output Voltage (V)', color: '#F59E0B' },
            { key: 'busVoltage', name: 'Bus Voltage (V)', color: '#EF4444' }
          ]
        }
      case 'temperature':
        return {
          lines: [
            { key: 'inverterTemp', name: 'Inverter Temperature (°C)', color: '#EF4444' }
          ]
        }
      default:
        return {
          lines: []
        }
    }
  }

  const config = getChartConfig()
  const formattedData = formatData(data)

  const getChartTitle = () => {
    switch(type) {
      case 'power':
        return 'Power Statistics'
      case 'current':
        return 'Battery Current'
      case 'voltage':
        return 'System Voltage'
      case 'temperature':
        return 'Temp Stats'
      default:
        return 'Statistics'
    }
  }

  return (
    <div className="h-full">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-4">
        <h2 className="text-xl sm:text-2xl font-semibold text-white">{getChartTitle()}</h2>
        <div className="flex flex-wrap gap-2">
          <button
            onClick={() => onTimeRangeChange('today')}
            className={`px-4 py-1.5 text-sm font-medium rounded-lg transition-all duration-200 ${
              timeRange === 'today'
                ? 'bg-gradient-to-r from-blue-500 to-blue-600 text-white shadow-lg shadow-blue-500/20 hover:shadow-blue-500/30'
                : 'bg-gray-700/50 text-gray-300 hover:bg-gray-600/50 hover:text-white hover:shadow-lg hover:shadow-gray-500/10'
            }`}
          >
            Today
          </button>
          <button
            onClick={() => onTimeRangeChange('yesterday')}
            className={`px-4 py-1.5 text-sm font-medium rounded-lg transition-all duration-200 ${
              timeRange === 'yesterday'
                ? 'bg-gradient-to-r from-blue-500 to-blue-600 text-white shadow-lg shadow-blue-500/20 hover:shadow-blue-500/30'
                : 'bg-gray-700/50 text-gray-300 hover:bg-gray-600/50 hover:text-white hover:shadow-lg hover:shadow-gray-500/10'
            }`}
          >
            Yesterday
          </button>
          <button
            onClick={() => onTimeRangeChange('week')}
            className={`px-4 py-1.5 text-sm font-medium rounded-lg transition-all duration-200 ${
              timeRange === 'week'
                ? 'bg-gradient-to-r from-blue-500 to-blue-600 text-white shadow-lg shadow-blue-500/20 hover:shadow-blue-500/30'
                : 'bg-gray-700/50 text-gray-300 hover:bg-gray-600/50 hover:text-white hover:shadow-lg hover:shadow-gray-500/10'
            }`}
          >
            Week
          </button>
          <button
            onClick={() => onTimeRangeChange('month')}
            className={`px-4 py-1.5 text-sm font-medium rounded-lg transition-all duration-200 ${
              timeRange === 'month'
                ? 'bg-gradient-to-r from-blue-500 to-blue-600 text-white shadow-lg shadow-blue-500/20 hover:shadow-blue-500/30'
                : 'bg-gray-700/50 text-gray-300 hover:bg-gray-600/50 hover:text-white hover:shadow-lg hover:shadow-gray-500/10'
            }`}
          >
            Month
          </button>
        </div>
      </div>
      <div className="h-[400px] w-full relative">
        <div className="absolute inset-0 bg-gradient-to-b from-transparent via-gray-900/5 to-transparent pointer-events-none" />
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={formattedData} margin={{ top: 10, right: 20, left: 0, bottom: 10 }}>
            <defs>
              {config.lines.map(line => (
                <linearGradient key={line.key} id={`gradient-${line.key}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={line.color} stopOpacity={0.4}/>
                  <stop offset="95%" stopColor={line.color} stopOpacity={0}/>
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid 
              strokeDasharray="3 3" 
              stroke="#374151" 
              opacity={0.1}
              vertical={false}
            />
            <XAxis 
              dataKey="timestamp" 
              stroke="#6B7280"
              tick={{ fill: '#6B7280', fontSize: 11 }}
              tickLine={{ stroke: '#6B7280' }}
              interval="preserveStartEnd"
              minTickGap={50}
              tickFormatter={(value) => {
                const date = new Date(value)
                if (timeRange === 'today' || timeRange === 'yesterday') {
                  return date.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', hour12: true })
                } else if (timeRange === 'week') {
                  return date.toLocaleDateString('en-US', { weekday: 'short', hour: 'numeric', hour12: true })
                } else {
                  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
                }
              }}
            />
            <YAxis 
              stroke="#6B7280"
              tick={{ fill: '#6B7280', fontSize: 11 }}
              tickLine={{ stroke: '#6B7280' }}
              axisLine={false}
            />
            <Tooltip 
              contentStyle={{ 
                backgroundColor: 'rgba(31, 41, 55, 0.95)',
                border: 'none',
                borderRadius: '0.5rem',
                color: '#F3F4F6',
                boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)',
                fontSize: '11px',
                backdropFilter: 'blur(8px)'
              }}
              labelStyle={{ color: '#F3F4F6', fontSize: '11px' }}
              cursor={{ stroke: 'rgba(255, 255, 255, 0.1)', strokeWidth: 2 }}
            />
            <Legend 
              wrapperStyle={{ 
                paddingTop: '0.5rem',
                color: '#6B7280',
                fontSize: '10px'
              }}
              iconType="circle"
              iconSize={8}
            />
            {config.lines.map(line => (
              <Line
                key={line.key}
                type="monotone"
                dataKey={line.key}
                name={line.name}
                stroke={line.color}
                strokeWidth={2}
                dot={false}
                activeDot={{ 
                  r: 6, 
                  fill: line.color, 
                  strokeWidth: 2, 
                  stroke: 'rgba(255, 255, 255, 0.2)',
                  filter: 'drop-shadow(0 0 4px rgba(255, 255, 255, 0.2))'
                }}
                animationDuration={1500}
                animationBegin={0}
                strokeLinecap="round"
                strokeLinejoin="round"
                isAnimationActive={true}
                animationEasing="ease-out"
                fill={`url(#gradient-${line.key})`}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
} 