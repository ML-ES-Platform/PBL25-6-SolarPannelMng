import mongoose from 'mongoose';

const predictionSchema = new mongoose.Schema({
  date: {
    type: Date,
    required: true,
    index: true
  },
  predictedPower: {
    type: Number,
    required: true,
    min: 0
  },
  weatherData: {
    date: String,
    temp_min: Number,
    temp_max: Number,
    humidity: Number,
    wind_speed: Number,
    icon: String,
    description: String,
    timestamp: Date
  },
  confidence: {
    type: Number,
    required: true,
    min: 0,
    max: 1
  },
  factors: {
    cloudImpact: {
      type: Number,
      required: true,
      min: 0,
      max: 1
    },
    tempImpact: {
      type: Number,
      required: true,
      min: 0,
      max: 1
    },
    rainImpact: {
      type: Number,
      required: true,
      min: 0,
      max: 1
    },
    weatherFactor: {
      type: Number,
      required: true,
      min: 0,
      max: 1
    }
  },
  lastUpdated: {
    type: Date,
    default: Date.now,
    index: true
  }
}, {
  timestamps: true
});

predictionSchema.index({ date: 1, lastUpdated: -1 });

predictionSchema.methods.isStale = function() {
  const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000);
  return this.lastUpdated < sixHoursAgo;
};

predictionSchema.statics.getFreshPredictions = function() {
  const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000);
  return this.find({ lastUpdated: { $gte: sixHoursAgo } }).sort({ date: 1 });
};

export const Prediction = mongoose.models.Prediction || mongoose.model('Prediction', predictionSchema);