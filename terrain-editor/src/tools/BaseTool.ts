import * as THREE from 'three';
import { Tool, BrushConfig } from '../types';
import { TerrainMesh } from '../terrain/TerrainMesh';

export abstract class BaseTool implements Tool {
  protected terrainMesh: TerrainMesh;
  protected scene: THREE.Scene;
  protected brushPreview: THREE.Mesh | null = null;
  protected isActive: boolean = false;

  constructor(
    public name: string,
    terrainMesh: TerrainMesh,
    scene: THREE.Scene,
    public icon?: string
  ) {
    this.terrainMesh = terrainMesh;
    this.scene = scene;
  }

  abstract apply(position: THREE.Vector3, brushConfig: BrushConfig, deltaTime: number): void;

  onActivate(): void {
    this.isActive = true;
    this.createBrushPreview();
  }

  onDeactivate(): void {
    this.isActive = false;
    this.removeBrushPreview();
  }

  updateBrushPreview(position: THREE.Vector3, brushConfig: BrushConfig): void {
    if (!this.brushPreview) return;

    // Update position
    this.brushPreview.position.copy(position);

    // Update scale based on brush size
    const scale = brushConfig.size * 2;
    this.brushPreview.scale.set(scale, scale * 0.5, scale);
  }

  protected createBrushPreview(): void {
    // Create a translucent sphere to show brush area
    const geometry = new THREE.SphereGeometry(1, 16, 16);
    const material = new THREE.MeshBasicMaterial({
      color: 0x00ff00,
      transparent: true,
      opacity: 0.3,
      wireframe: true
    });

    this.brushPreview = new THREE.Mesh(geometry, material);
    this.scene.add(this.brushPreview);
  }

  protected removeBrushPreview(): void {
    if (this.brushPreview) {
      this.scene.remove(this.brushPreview);
      this.brushPreview.geometry.dispose();
      (this.brushPreview.material as THREE.Material).dispose();
      this.brushPreview = null;
    }
  }

  protected applyHeightChange(
    gridPosition: { x: number; y: number },
    brushConfig: BrushConfig,
    deltaTime: number,
    direction: number // 1 for fill, -1 for dig
  ): void {
    const config = this.terrainMesh.getConfig();
    const radius = Math.floor(brushConfig.size);
    const strength = brushConfig.strength * deltaTime * direction;

    // Apply height changes in a circular area
    for (let dy = -radius; dy <= radius; dy++) {
      for (let dx = -radius; dx <= radius; dx++) {
        const x = gridPosition.x + dx;
        const y = gridPosition.y + dy;

        // Check bounds
        if (x < 0 || x > config.widthSegments || y < 0 || y > config.heightSegments) {
          continue;
        }

        // Calculate distance from center
        const distance = Math.sqrt(dx * dx + dy * dy);
        if (distance > radius) continue;

        // Calculate falloff
        let falloff = 1.0;
        if (brushConfig.falloff === 'gaussian') {
          // Gaussian falloff
          const sigma = radius / 3;
          falloff = Math.exp(-(distance * distance) / (2 * sigma * sigma));
        } else if (brushConfig.falloff === 'linear') {
          // Linear falloff
          falloff = 1.0 - (distance / radius);
        }
        // constant falloff is 1.0

        // Get current height and apply change
        const currentHeight = this.terrainMesh.getHeightAt(x, y);
        const heightChange = strength * falloff * 10; // Scale factor for visible effect
        const newHeight = currentHeight + heightChange;

        // Set new height
        this.terrainMesh.setHeightAt(x, y, newHeight);
      }
    }

    // Update the mesh geometry
    this.terrainMesh.updateHeightmap(this.terrainMesh.getHeightmap());
  }
} 