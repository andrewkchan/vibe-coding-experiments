import { Vector3 } from 'three';

export interface TerrainConfig {
  width: number;
  height: number;
  widthSegments: number;
  heightSegments: number;
  maxHeight: number;
  minHeight: number;
}

export interface NoiseConfig {
  scale: number;
  octaves: number;
  persistence: number;
  lacunarity: number;
  seed: number;
}

export interface BrushConfig {
  size: number;
  strength: number;
  falloff: 'gaussian' | 'linear' | 'constant';
}

export interface Tool {
  name: string;
  icon?: string;
  apply(position: Vector3, brushConfig: BrushConfig, deltaTime: number): void;
  onActivate(): void;
  onDeactivate(): void;
}

export interface CameraConfig {
  moveSpeed: number;
  lookSpeed: number;
  verticalSpeed: number;
  acceleration: number;
  friction: number;
}

export interface InputState {
  keys: Set<string>;
  mouseDown: boolean;
  mousePosition: { x: number; y: number };
  mouseDelta: { x: number; y: number };
}

export type ToolType = 'fill' | 'dig' | 'smooth' | 'flatten' | 'noise'; 