import * as THREE from 'three';
import { BaseTool } from './BaseTool';
import { BrushConfig } from '../types';

export class DigTool extends BaseTool {
  apply(position: THREE.Vector3, brushConfig: BrushConfig, deltaTime: number): void {
    // Convert world position to grid coordinates
    const gridPos = this.terrainMesh.worldToGrid(position.x, position.z);
    if (!gridPos) return;

    // Apply height change with negative direction
    this.applyHeightChange(gridPos, brushConfig, deltaTime, -1);
  }

  protected createBrushPreview(): void {
    super.createBrushPreview();
    // Make dig tool preview reddish
    if (this.brushPreview) {
      (this.brushPreview.material as THREE.MeshBasicMaterial).color.setHex(0xff0000);
    }
  }
} 