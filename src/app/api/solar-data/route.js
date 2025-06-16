import { NextResponse } from 'next/server'
import { connectToDatabase } from '@/lib/mongodb'

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const timeRange = searchParams.get('timeRange') || 'today'
  const since = searchParams.get('since')

  try {
    const { db } = await connectToDatabase()
    const collection = db.collection('solar_data')
    
    const now = new Date()
    let startDate = new Date()
    let endDate = new Date()

    if (since) {
      startDate = new Date(since)
    } else {
      switch (timeRange) {
        case 'today':
          startDate.setHours(0, 0, 0, 0)
          break
        case 'yesterday':
          startDate.setDate(now.getDate() - 1)
          startDate.setHours(0, 0, 0, 0)
          endDate = new Date(startDate)
          endDate.setHours(23, 59, 59, 999)
          break
        case 'week':
          startDate.setDate(now.getDate() - 7)
          startDate.setHours(0, 0, 0, 0)
          break
        case 'month':
          startDate.setMonth(now.getMonth() - 1)
          startDate.setHours(0, 0, 0, 0)
          break
        default:
          startDate.setHours(0, 0, 0, 0)
      }
    }

    const data = await collection.find({
      timestamp: { 
        $gte: startDate,
        $lte: endDate
      }
    }).sort({ timestamp: 1 }).toArray()

    return NextResponse.json(data)
  } catch (error) {
    console.error('Error fetching data:', error)
    return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 })
  }
} 