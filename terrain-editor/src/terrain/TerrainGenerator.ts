import { createNoise2D } from 'simplex-noise';
import { TerrainConfig, NoiseConfig } from '../types';

export class TerrainGenerator {
  private noise2D: ReturnType<typeof createNoise2D>;
  
  constructor(seed?: number) {
    // Create a seeded random function if seed is provided
    const random = seed !== undefined ? this.mulberry32(seed) : Math.random;
    this.noise2D = createNoise2D(random);
  }

  generateHeightmap(
    config: TerrainConfig,
    noiseConfig: NoiseConfig
  ): Float32Array {
    const { widthSegments, heightSegments, maxHeight } = config;
    const size = (widthSegments + 1) * (heightSegments + 1);
    const heightmap = new Float32Array(size);

    for (let y = 0; y <= heightSegments; y++) {
      for (let x = 0; x <= widthSegments; x++) {
        const index = y * (widthSegments + 1) + x;
        
        // Normalize coordinates to [0, 1]
        const nx = x / widthSegments;
        const ny = y / heightSegments;
        
        // Generate height using multiple octaves of noise
        let height = 0;
        let amplitude = 1;
        let frequency = 1;
        let maxValue = 0; // Used for normalizing result to [0, 1]
        
        for (let i = 0; i < noiseConfig.octaves; i++) {
          const sampleX = nx * noiseConfig.scale * frequency;
          const sampleY = ny * noiseConfig.scale * frequency;
          
          height += this.noise2D(sampleX, sampleY) * amplitude;
          
          maxValue += amplitude;
          amplitude *= noiseConfig.persistence;
          frequency *= noiseConfig.lacunarity;
        }
        
        // Normalize to [0, 1] then scale to max height
        height = (height / maxValue + 1) * 0.5; // Convert from [-1, 1] to [0, 1]
        
        // Apply edge falloff to create bounded terrain
        const edgeFalloff = this.calculateEdgeFalloff(nx, ny);
        height *= edgeFalloff;
        
        heightmap[index] = height * maxHeight;
      }
    }
    
    return heightmap;
  }

  private calculateEdgeFalloff(x: number, y: number): number {
    // Distance from center (0.5, 0.5)
    const dx = Math.abs(x - 0.5) * 2;
    const dy = Math.abs(y - 0.5) * 2;
    const d = Math.max(dx, dy); // Square distance
    
    // Smooth falloff starting at 0.7 from center
    const falloffStart = 0.7;
    const falloffEnd = 1.0;
    
    if (d <= falloffStart) {
      return 1.0;
    } else if (d >= falloffEnd) {
      return 0.0;
    } else {
      // Smooth interpolation
      const t = (d - falloffStart) / (falloffEnd - falloffStart);
      return 1.0 - t * t * (3.0 - 2.0 * t); // Smoothstep
    }
  }

  // Mulberry32 - a simple seedable random number generator
  private mulberry32(seed: number): () => number {
    return function() {
      let t = seed += 0x6D2B79F5;
      t = Math.imul(t ^ t >>> 15, t | 1);
      t ^= t + Math.imul(t ^ t >>> 7, t | 61);
      return ((t ^ t >>> 14) >>> 0) / 4294967296;
    };
  }
} 