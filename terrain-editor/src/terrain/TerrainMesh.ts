import * as THREE from 'three';
import { TerrainConfig } from '../types';

export class TerrainMesh {
  private geometry: THREE.PlaneGeometry;
  private material: THREE.MeshBasicMaterial;
  public mesh: THREE.Mesh;
  private config: TerrainConfig;
  private heightmap: Float32Array;

  constructor(config: TerrainConfig) {
    this.config = config;
    
    // Create plane geometry
    this.geometry = new THREE.PlaneGeometry(
      config.width,
      config.height,
      config.widthSegments,
      config.heightSegments
    );

    // Create wireframe material
    this.material = new THREE.MeshBasicMaterial({
      color: 0x00ff00,
      wireframe: true,
      wireframeLinewidth: 1,
    });

    // Create mesh
    this.mesh = new THREE.Mesh(this.geometry, this.material);
    
    // Rotate to be horizontal (by default PlaneGeometry is vertical)
    this.mesh.rotation.x = -Math.PI / 2;
    
    // Initialize heightmap
    const vertexCount = (config.widthSegments + 1) * (config.heightSegments + 1);
    this.heightmap = new Float32Array(vertexCount);
  }

  updateHeightmap(heightmap: Float32Array): void {
    this.heightmap = heightmap;
    this.updateGeometry();
  }

  setHeightAt(x: number, y: number, height: number): void {
    const index = y * (this.config.widthSegments + 1) + x;
    if (index >= 0 && index < this.heightmap.length) {
      this.heightmap[index] = Math.max(
        this.config.minHeight,
        Math.min(this.config.maxHeight, height)
      );
    }
  }

  getHeightAt(x: number, y: number): number {
    const index = y * (this.config.widthSegments + 1) + x;
    if (index >= 0 && index < this.heightmap.length) {
      return this.heightmap[index];
    }
    return 0;
  }

  getHeightAtWorldPosition(worldX: number, worldZ: number): number {
    // Convert world coordinates to grid coordinates
    const gridX = Math.round(
      ((worldX + this.config.width / 2) / this.config.width) * this.config.widthSegments
    );
    const gridY = Math.round(
      ((worldZ + this.config.height / 2) / this.config.height) * this.config.heightSegments
    );
    
    return this.getHeightAt(gridX, gridY);
  }

  worldToGrid(worldX: number, worldZ: number): { x: number; y: number } | null {
    const x = Math.round(
      ((worldX + this.config.width / 2) / this.config.width) * this.config.widthSegments
    );
    const y = Math.round(
      ((worldZ + this.config.height / 2) / this.config.height) * this.config.heightSegments
    );
    
    // Check bounds
    if (x < 0 || x > this.config.widthSegments || y < 0 || y > this.config.heightSegments) {
      return null;
    }
    
    return { x, y };
  }

  gridToWorld(gridX: number, gridY: number): { x: number; z: number } {
    const x = (gridX / this.config.widthSegments) * this.config.width - this.config.width / 2;
    const z = (gridY / this.config.heightSegments) * this.config.height - this.config.height / 2;
    return { x, z };
  }

  private updateGeometry(): void {
    const positions = this.geometry.attributes.position;
    
    for (let i = 0; i < this.heightmap.length; i++) {
      positions.setZ(i, this.heightmap[i]);
    }
    
    // Update normals and bounding sphere
    this.geometry.computeVertexNormals();
    this.geometry.attributes.position.needsUpdate = true;
    this.geometry.computeBoundingSphere();
  }

  getHeightmap(): Float32Array {
    return this.heightmap.slice();
  }

  getConfig(): TerrainConfig {
    return { ...this.config };
  }

  dispose(): void {
    this.geometry.dispose();
    this.material.dispose();
  }
} 