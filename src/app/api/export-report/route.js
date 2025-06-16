import { NextResponse } from 'next/server'
import { connectToDatabase } from '@/lib/mongodb'
import { PDFDocument, rgb, StandardFonts } from 'pdf-lib'
import QuickChart from 'quickchart-js'

function calculateStatistics(data) {
  const stats = {
    battery: {
      avgCapacity: 0,
      minCapacity: Infinity,
      maxCapacity: 0,
      avgVoltage: 0,
      chargingTime: 0,
      totalChargingTime: 0
    },
    power: {
      avgPvInput: 0,
      maxPvInput: 0,
      totalPvEnergy: 0,
      avgAcOutput: 0,
      maxAcOutput: 0,
      totalAcEnergy: 0
    },
    temperature: {
      avgTemp: 0,
      maxTemp: 0,
      minTemp: Infinity
    }
  }

  let lastTimestamp = null
  let isCharging = false
  let chargingStart = null

  data.forEach((item, index) => {
    const currentData = item.data
    const timestamp = new Date(item.timestamp.$date || item.timestamp)
    
    stats.battery.avgCapacity += currentData.battery_capacity
    stats.battery.minCapacity = Math.min(stats.battery.minCapacity, currentData.battery_capacity)
    stats.battery.maxCapacity = Math.max(stats.battery.maxCapacity, currentData.battery_capacity)
    stats.battery.avgVoltage += currentData.battery_voltage

    stats.power.avgPvInput += currentData.pv_input_power
    stats.power.maxPvInput = Math.max(stats.power.maxPvInput, currentData.pv_input_power)
    stats.power.avgAcOutput += currentData.ac_output_active_power
    stats.power.maxAcOutput = Math.max(stats.power.maxAcOutput, currentData.ac_output_active_power)

    stats.temperature.avgTemp += currentData.inverter_heat_sink_temperature
    stats.temperature.maxTemp = Math.max(stats.temperature.maxTemp, currentData.inverter_heat_sink_temperature)
    stats.temperature.minTemp = Math.min(stats.temperature.minTemp, currentData.inverter_heat_sink_temperature)

    if (lastTimestamp) {
      const timeDiff = (timestamp - lastTimestamp) / (1000 * 60 * 60) // hours
      stats.power.totalPvEnergy += (currentData.pv_input_power * timeDiff)
      stats.power.totalAcEnergy += (currentData.ac_output_active_power * timeDiff)
    }

    if (currentData.battery_charging_current > 0) {
      if (!isCharging) {
        chargingStart = timestamp
        isCharging = true
      }
    } else {
      if (isCharging && chargingStart) {
        stats.battery.chargingTime += (timestamp - chargingStart) / (1000 * 60)
        isCharging = false
        chargingStart = null
      }
    }

    lastTimestamp = timestamp
  })

  const count = data.length
  stats.battery.avgCapacity /= count
  stats.battery.avgVoltage /= count
  stats.power.avgPvInput /= count
  stats.power.avgAcOutput /= count
  stats.temperature.avgTemp /= count

  return stats
}

async function generateCharts(data) {
  const timestamps = data.map((item, index) => {
    if (index % Math.ceil(data.length / 6) !== 0) return ''
    const date = new Date(item.timestamp.$date || item.timestamp)
    return date.toLocaleTimeString('en-US', {
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    })
  })
  
  const batteryCapacityChart = new QuickChart()
  batteryCapacityChart.setWidth(800)
  batteryCapacityChart.setHeight(400)
  batteryCapacityChart.setConfig({
    type: 'line',
    data: {
      labels: timestamps,
      datasets: [{
        label: 'Battery Capacity (%)',
        data: data.map(item => item.data.battery_capacity),
        borderColor: '#1E40AF',
        backgroundColor: 'rgba(30, 64, 175, 0.1)',
        fill: true,
        tension: 0.4
      }]
    },
    options: {
      plugins: {
        title: {
          display: true,
          text: 'Battery Capacity Over Time',
          font: { size: 16 }
        }
      },
      scales: {
        x: {
          ticks: {
            maxRotation: 0,
            font: { size: 10 },
            padding: 10
          },
          grid: {
            display: false
          }
        },
        y: {
          beginAtZero: true,
          max: 100,
          grid: {
            color: 'rgba(0, 0, 0, 0.1)'
          }
        }
      }
    }
  })

  const powerChart = new QuickChart()
  powerChart.setWidth(800)
  powerChart.setHeight(400)
  powerChart.setConfig({
    type: 'line',
    data: {
      labels: timestamps,
      datasets: [
        {
          label: 'PV Input Power (W)',
          data: data.map(item => item.data.pv_input_power),
          borderColor: '#059669',
          backgroundColor: 'rgba(5, 150, 105, 0.1)',
          fill: true,
          tension: 0.4
        },
        {
          label: 'AC Output Power (W)',
          data: data.map(item => item.data.ac_output_active_power),
          borderColor: '#DC2626',
          backgroundColor: 'rgba(220, 38, 38, 0.1)',
          fill: true,
          tension: 0.4
        }
      ]
    },
    options: {
      plugins: {
        title: {
          display: true,
          text: 'Power Generation and Consumption',
          font: { size: 16 }
        }
      },
      scales: {
        x: {
          ticks: {
            maxRotation: 0,
            font: { size: 10 },
            padding: 10
          },
          grid: {
            display: false
          }
        },
        y: {
          grid: {
            color: 'rgba(0, 0, 0, 0.1)'
          }
        }
      }
    }
  })

  const temperatureChart = new QuickChart()
  temperatureChart.setWidth(800)
  temperatureChart.setHeight(400)
  temperatureChart.setConfig({
    type: 'line',
    data: {
      labels: timestamps,
      datasets: [{
        label: 'Inverter Temperature (°C)',
        data: data.map(item => item.data.inverter_heat_sink_temperature),
        borderColor: '#D97706',
        backgroundColor: 'rgba(217, 119, 6, 0.1)',
        fill: true,
        tension: 0.4
      }]
    },
    options: {
      plugins: {
        title: {
          display: true,
          text: 'Inverter Temperature Over Time',
          font: { size: 16 }
        }
      },
      scales: {
        x: {
          ticks: {
            maxRotation: 0,
            font: { size: 10 },
            padding: 10
          },
          grid: {
            display: false
          }
        },
        y: {
          grid: {
            color: 'rgba(0, 0, 0, 0.1)'
          }
        }
      }
    }
  })

  const batteryCapacityUrl = batteryCapacityChart.getUrl()
  const powerUrl = powerChart.getUrl()
  const temperatureUrl = temperatureChart.getUrl()

  const batteryCapacityResponse = await fetch(batteryCapacityUrl)
  const powerResponse = await fetch(powerUrl)
  const temperatureResponse = await fetch(temperatureUrl)

  const batteryCapacityBuffer = Buffer.from(await batteryCapacityResponse.arrayBuffer())
  const powerBuffer = Buffer.from(await powerResponse.arrayBuffer())
  const temperatureBuffer = Buffer.from(await temperatureResponse.arrayBuffer())

  return { 
    batteryCapacityImage: batteryCapacityBuffer, 
    powerImage: powerBuffer, 
    temperatureImage: temperatureBuffer 
  }
}

async function generatePDF(stats, timeRange, data) {
  const pdfDoc = await PDFDocument.create()
  
  const font = await pdfDoc.embedFont(StandardFonts.Helvetica)
  const boldFont = await pdfDoc.embedFont(StandardFonts.HelveticaBold)
  
  const page1 = pdfDoc.addPage([595.28, 841.89]) // A4 size
  const page2 = pdfDoc.addPage([595.28, 841.89])
  const page3 = pdfDoc.addPage([595.28, 841.89])
  
  const { width, height } = page1.getSize()
  
  const titleFontSize = 24
  const headerFontSize = 18
  const textFontSize = 12
  const footerFontSize = 10
  const margin = 50
  const lineHeight = 20
  
  const drawHeader = (page, title) => {
    page.drawRectangle({
      x: 0,
      y: 0,
      width,
      height,
      color: rgb(0.95, 0.95, 0.95)
    })
    
    page.drawText(title, {
      x: width / 2 - 100,
      y: height - 50,
      size: titleFontSize,
      font: boldFont,
      color: rgb(0.12, 0.25, 0.67) // #1E40AF
    })
    
    page.drawText(`Time Period: ${timeRange.charAt(0).toUpperCase() + timeRange.slice(1)}`, {
      x: width / 2 - 80,
      y: height - 80,
      size: textFontSize,
      font,
      color: rgb(0.29, 0.34, 0.39) // #4B5563
    })
  }
  
  const drawFooter = (page) => {
    page.drawText(`Generated on ${new Date().toLocaleString()}`, {
      x: width / 2 - 100,
      y: 50,
      size: footerFontSize,
      font,
      color: rgb(0.42, 0.45, 0.5) // #6B7280
    })
  }
  
  drawHeader(page1, 'Solar Panel Report')
  
  const charts = await generateCharts(data)
  const batteryCapacityPdfImage = await pdfDoc.embedPng(charts.batteryCapacityImage)
  page1.drawImage(batteryCapacityPdfImage, {
    x: margin,
    y: height - 400,
    width: width - (2 * margin),
    height: 300
  })
  
  const batteryStats = [
    `Average Battery Capacity: ${stats.battery.avgCapacity.toFixed(1)}%`,
    `Minimum Battery Capacity: ${stats.battery.minCapacity.toFixed(1)}%`,
    `Maximum Battery Capacity: ${stats.battery.maxCapacity.toFixed(1)}%`,
    `Average Battery Voltage: ${stats.battery.avgVoltage.toFixed(1)}V`,
    `Total Charging Time: ${(stats.battery.chargingTime / 60).toFixed(1)} hours`
  ]
  
  page1.drawText('Battery Statistics', {
    x: margin,
    y: height - 450,
    size: headerFontSize,
    font: boldFont,
    color: rgb(0.12, 0.25, 0.67)
  })
  
  batteryStats.forEach((text, index) => {
    page1.drawText(text, {
      x: margin,
      y: height - 500 - (index * lineHeight),
      size: textFontSize,
      font,
      color: rgb(0.29, 0.34, 0.39)
    })
  })
  
  drawFooter(page1)
  
  drawHeader(page2, 'Power Analysis')
  
  const powerPdfImage = await pdfDoc.embedPng(charts.powerImage)
  page2.drawImage(powerPdfImage, {
    x: margin,
    y: height - 400,
    width: width - (2 * margin),
    height: 300
  })
  
  const powerStats = [
    `Average PV Input Power: ${stats.power.avgPvInput.toFixed(1)}W`,
    `Maximum PV Input Power: ${stats.power.maxPvInput.toFixed(1)}W`,
    `Total PV Energy Generated: ${stats.power.totalPvEnergy.toFixed(1)}Wh`,
    `Average AC Output Power: ${stats.power.avgAcOutput.toFixed(1)}W`,
    `Maximum AC Output Power: ${stats.power.maxAcOutput.toFixed(1)}W`,
    `Total AC Energy Consumed: ${stats.power.totalAcEnergy.toFixed(1)}Wh`
  ]
  
  page2.drawText('Power Statistics', {
    x: margin,
    y: height - 450,
    size: headerFontSize,
    font: boldFont,
    color: rgb(0.12, 0.25, 0.67)
  })
  
  powerStats.forEach((text, index) => {
    page2.drawText(text, {
      x: margin,
      y: height - 500 - (index * lineHeight),
      size: textFontSize,
      font,
      color: rgb(0.29, 0.34, 0.39)
    })
  })
  
  drawFooter(page2)
  
  drawHeader(page3, 'System Health')
  
  const temperaturePdfImage = await pdfDoc.embedPng(charts.temperatureImage)
  page3.drawImage(temperaturePdfImage, {
    x: margin,
    y: height - 400,
    width: width - (2 * margin),
    height: 300
  })
  
  const tempStats = [
    `Average Inverter Temperature: ${stats.temperature.avgTemp.toFixed(1)}°C`,
    `Maximum Inverter Temperature: ${stats.temperature.maxTemp.toFixed(1)}°C`,
    `Minimum Inverter Temperature: ${stats.temperature.minTemp.toFixed(1)}°C`
  ]
  
  page3.drawText('Temperature Statistics', {
    x: margin,
    y: height - 450,
    size: headerFontSize,
    font: boldFont,
    color: rgb(0.12, 0.25, 0.67)
  })
  
  tempStats.forEach((text, index) => {
    page3.drawText(text, {
      x: margin,
      y: height - 500 - (index * lineHeight),
      size: textFontSize,
      font,
      color: rgb(0.29, 0.34, 0.39)
    })
  })
  
  const healthSummary = [
    'System Health Summary:',
    `• Battery Health: ${stats.battery.avgCapacity > 80 ? 'Excellent' : stats.battery.avgCapacity > 60 ? 'Good' : 'Needs Attention'}`,
    `• Power Generation: ${stats.power.totalPvEnergy > 1000 ? 'High' : stats.power.totalPvEnergy > 500 ? 'Moderate' : 'Low'}`,
    `• Temperature Status: ${stats.temperature.avgTemp < 40 ? 'Normal' : stats.temperature.avgTemp < 50 ? 'Warning' : 'Critical'}`
  ]
  
  page3.drawText('System Health Summary', {
    x: margin,
    y: height - 600,
    size: headerFontSize,
    font: boldFont,
    color: rgb(0.12, 0.25, 0.67)
  })
  
  healthSummary.forEach((text, index) => {
    page3.drawText(text, {
      x: margin,
      y: height - 650 - (index * lineHeight),
      size: textFontSize,
      font,
      color: rgb(0.29, 0.34, 0.39)
    })
  })
  
  drawFooter(page3)
  
  return await pdfDoc.save()
}

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const timeRange = searchParams.get('timeRange') || 'today'

  try {
    const { db } = await connectToDatabase()
    const collection = db.collection('solar_data')
    
    const now = new Date()
    let startDate = new Date()
    let endDate = new Date()

    switch (timeRange) {
      case 'today':
        startDate.setHours(0, 0, 0, 0)
        endDate = now
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
        endDate = now
        break
      case 'month':
        startDate.setMonth(now.getMonth() - 1)
        startDate.setHours(0, 0, 0, 0)
        endDate = now
        break
      default:
        startDate.setHours(0, 0, 0, 0)
        endDate = now
    }

    const data = await collection.find({
      timestamp: { 
        $gte: startDate,
        $lte: endDate
      }
    }).sort({ timestamp: 1 }).toArray()

    const stats = calculateStatistics(data)
    const pdfBytes = await generatePDF(stats, timeRange, data)

    return new NextResponse(pdfBytes, {
      headers: {
        'Content-Type': 'application/pdf',
        'Content-Disposition': `attachment; filename=solar-report-${timeRange}.pdf`
      }
    })
  } catch (error) {
    console.error('Error generating report:', error)
    return NextResponse.json({ error: 'Failed to generate report' }, { status: 500 })
  }
} 