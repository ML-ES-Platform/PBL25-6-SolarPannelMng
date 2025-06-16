import { Card } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { motion } from 'framer-motion'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

export function BatteryStatus({ data, history }) {
  const batteryLevel = data.battery_capacity || 0
  const batteryVoltage = data.battery_voltage || 0
  const chargingCurrent = data.battery_charging_current || 0
  const isCharging = data.is_charging_on === 1
  const isChargingToFloat = data.is_charging_to_float === 1
  const batteryVoltageFromScc = data.battery_voltage_from_scc || 0
  const batteryDischargeCurrent = data.battery_discharge_current || 0
  const inverterTemp = data.inverter_heat_sink_temperature || 0

  const getBatteryColor = (level) => {
    if (level >= 80) return 'bg-emerald-500'
    if (level >= 40) return 'bg-yellow-500'
    return 'bg-red-500'
  }

  const getBatteryGradient = (level) => {
    if (level >= 80) return 'from-emerald-500 to-emerald-400'
    if (level >= 40) return 'from-yellow-500 to-yellow-400'
    return 'from-red-500 to-red-400'
  }

  const formatHistoryData = (history) => {
    return history.map(item => {
      const dateStr = item.timestamp.$date || item.timestamp
      const date = new Date(dateStr)
      return {
        timestamp: date.toLocaleString('en-US', {
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          hour12: true
        }),
        capacity: item.data.battery_capacity
      }
    })
  }

  const MiniChart = ({ data, dataKey, color, height = 40 }) => {
    if (!data || data.length === 0) return null
    return (
      <div className={`h-[${height}px] w-full mt-1`}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 2, right: 2, left: 2, bottom: 2 }}>
            <Line
              type="monotone"
              dataKey={dataKey}
              stroke={color}
              strokeWidth={1}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    )
  }

  return (
    <Card className="h-full p-4 sm:p-6 bg-gray-800/50 backdrop-blur-sm border-gray-700 hover:bg-gray-800/70 transition-all duration-300 transform hover:scale-[1.02] hover:shadow-2xl hover:shadow-emerald-500/10">
      <h2 className="text-xl sm:text-2xl font-semibold mb-4 text-white">Battery Status</h2>
      <div className="flex flex-col h-[calc(100%-3rem)]">
        <div className="relative mb-4">
          <div className="flex justify-between mb-2">
            <span className="text-sm font-medium text-gray-300">Battery Level</span>
            <span className="text-sm font-medium text-gray-300">{batteryLevel}%</span>
          </div>
          <div className="relative h-10 bg-gray-700/50 rounded-full overflow-hidden">
            <motion.div
              className={`absolute inset-0 bg-gradient-to-r ${getBatteryGradient(batteryLevel)}`}
              initial={{ width: 0 }}
              animate={{ width: `${batteryLevel}%` }}
              transition={{ duration: 1, ease: "easeOut" }}
            />
            <motion.div
              className="absolute inset-0 bg-gradient-to-r from-white/20 to-transparent"
              animate={{
                x: ["0%", `${batteryLevel}%`],
              }}
              transition={{
                duration: 2,
                repeat: Infinity,
                ease: "linear",
              }}
              style={{
                width: "50%",
                height: "100%",
                background: "linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent)",
                maxWidth: `${batteryLevel}%`,
              }}
            />
            {isCharging && (
              <div className="absolute inset-0 flex items-center justify-center">
                <motion.svg
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="0 0 24 24"
                  fill="currentColor"
                  className="w-6 h-6 text-white/80"
                  initial={{ scale: 0.8, opacity: 0 }}
                  animate={{ scale: 1, opacity: 1 }}
                  transition={{ duration: 0.3 }}
                >
                  <path fillRule="evenodd" d="M14.615 1.595a.75.75 0 01.359.852L12.982 9.75h7.268a.75.75 0 01.548 1.262l-10.5 11.25a.75.75 0 01-1.272-.71l1.992-7.302H3.75a.75.75 0 01-.548-1.262l10.5-11.25a.75.75 0 01.913-.143z" clipRule="evenodd" />
                </motion.svg>
              </div>
            )}
          </div>
          <div className="h-16 mt-2">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={formatHistoryData(history)} margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
                <defs>
                  <linearGradient id="capacityGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#10B981" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#10B981" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.1} />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: '#1F2937',
                    border: 'none',
                    borderRadius: '0.5rem',
                    color: '#F3F4F6',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)',
                    fontSize: '11px'
                  }}
                  labelStyle={{ color: '#F3F4F6', fontSize: '11px' }}
                  labelFormatter={(label, payload) => {
                    if (!payload || !payload[0] || !payload[0].payload) return label
                    return payload[0].payload.timestamp
                  }}
                  formatter={(value) => [`${value}%`, 'Battery Level']}
                />
                <Line
                  type="monotone"
                  dataKey="capacity"
                  name="Battery Level"
                  stroke="#10B981"
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4, fill: '#10B981' }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="grid grid-cols-2 gap-3 flex-grow">
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">Main Voltage</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{batteryVoltage}</p>
              <p className="text-sm font-medium text-blue-400">V</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">SCC Voltage</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{batteryVoltageFromScc}</p>
              <p className="text-sm font-medium text-blue-400">V</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">Charging Current</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{chargingCurrent}</p>
              <p className="text-sm font-medium text-emerald-400">A</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">Discharge Current</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{batteryDischargeCurrent}</p>
              <p className="text-sm font-medium text-emerald-400">A</p>
            </div>
          </div>
        </div>
        <div className="grid grid-cols-2 gap-2 mt-4">
          <div className="flex items-center gap-2">
            <div className={`w-3 h-3 rounded-full ${isCharging ? 'bg-emerald-500 animate-pulse' : 'bg-gray-400'}`} />
            <span className="text-sm text-gray-300">{isCharging ? 'Charging' : 'Not Charging'}</span>
          </div>
          <div className="flex items-center gap-2">
            <div className={`w-3 h-3 rounded-full ${isChargingToFloat ? 'bg-emerald-500' : 'bg-gray-400'}`} />
            <span className="text-sm text-gray-300">Float Mode</span>
          </div>
        </div>
      </div>
    </Card>
  )
} 