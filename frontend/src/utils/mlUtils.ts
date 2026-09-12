/**
 * ML Utility functions for processing and formatting GitPilot AI model outputs.
 */

export interface ModelPrediction {
  confidence: number;
  label: string;
  explanation: string;
  features: Record<string, number>;
}

/**
 * Formats a raw confidence score (0-1) into a user-friendly percentage string.
 */
export const formatConfidence = (score: number, decimalPlaces = 1): string => {
  return `${(score * 100).toFixed(decimalPlaces)}%`;
};

/**
 * Maps an AI-determined priority level to a corresponding UI color class.
 */
export const getPriorityColorMap = (priority: string): string => {
  switch (priority.toLowerCase()) {
    case 'critical': return 'text-red-700 bg-red-100 border-red-200';
    case 'high': return 'text-orange-700 bg-orange-100 border-orange-200';
    case 'medium': return 'text-blue-700 bg-blue-100 border-blue-200';
    case 'low': return 'text-green-700 bg-green-100 border-green-200';
    default: return 'text-gray-700 bg-gray-100 border-gray-200';
  }
};

/**
 * Calculates a complexity score based on issue content length and technical terminology density.
 */
export const calculateComplexity = (body: string): number => {
  const technicalTerms = ['error', 'exception', 'pointer', 'memory', 'segfault', 'async', 'deadlock', 'race condition'];
  const words = body.toLowerCase().split(/\s+/);
  
  let score = words.length * 0.1; 
  words.forEach(word => {
    if (technicalTerms.includes(word)) score += 5;
  });

  return Math.min(Math.round(score), 100);
};

/**
 * Evaluates semantic similarity between two text vectors (mock implementation).
 */
export const calculateSimilarityScore = (vecA: number[], vecB: number[]): number => {
  if (vecA.length !== vecB.length) return 0;
  let dotProduct = 0;
  let normA = 0;
  let normB = 0;
  for (let i = 0; i < vecA.length; i++) {
    dotProduct += vecA[i] * vecB[i];
    normA += vecA[i] * vecA[i];
    normB += vecB[i] * vecB[i];
  }
  if (normA === 0 || normB === 0) return 0;
  return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
};

/**
 * Parses raw ML explanation JSON strings into formatted human-readable bullet points.
 */
export const parseMLExplanation = (rawJson: string): string[] => {
  try {
    const parsed = JSON.parse(rawJson);
    if (Array.isArray(parsed.reasons)) {
      return parsed.reasons;
    }
    return ['No detailed explanation available.'];
  } catch (e) {
    return ['Failed to parse explanation data.'];
  }
};
