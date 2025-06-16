import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'

export function LastRefresh({ timestamp }) {
  const [relativeTime, setRelativeTime] = useState('')

  useEffect(() => {
    const updateRelativeTime = () => {
      const now = new Date()
      const lastRefresh = new Date(timestamp)
      const diffInSeconds = Math.floor((now - lastRefresh) / 1000)

      if (diffInSeconds < 60) {
        setRelativeTime(`${diffInSeconds} seconds ago`)
      } else if (diffInSeconds < 3600) {
        const minutes = Math.floor(diffInSeconds / 60)
        setRelativeTime(`${minutes} minute${minutes > 1 ? 's' : ''} ago`)
      } else {
        const hours = Math.floor(diffInSeconds / 3600)
        setRelativeTime(`${hours} hour${hours > 1 ? 's' : ''} ago`)
      }
    }

    updateRelativeTime()
    const interval = setInterval(updateRelativeTime, 1000)
    return () => clearInterval(interval)
  }, [timestamp])

  return (
    <motion.div
      initial={{ opacity: 0, y: -10 }}
      animate={{ opacity: 1, y: 0 }}
      className="flex items-center gap-2 text-sm text-gray-400"
    >
      <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
      <span>Last updated {relativeTime}</span>
    </motion.div>
  )
} 