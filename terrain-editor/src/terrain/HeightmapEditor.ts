import * as THREE from 'three';
import { TerrainMesh } from './TerrainMesh';
import { Tool, BrushConfig, ToolType } from '../types';
import { FillTool } from '../tools/FillTool';
import { DigTool } from '../tools/DigTool';

export class HeightmapEditor {
  private terrainMesh: TerrainMesh;
  private scene: THREE.Scene;
  private camera: THREE.Camera;
  private raycaster: THREE.Raycaster;
  private tools: Map<ToolType, Tool>;
  private currentTool: Tool | null = null;
  private isEditing: boolean = false;
  private brushConfig: BrushConfig;
  private mouse: THREE.Vector2;
  private intersectionPoint: THREE.Vector3 | null = null;

  constructor(
    terrainMesh: TerrainMesh,
    scene: THREE.Scene,
    camera: THREE.Camera
  ) {
    this.terrainMesh = terrainMesh;
    this.scene = scene;
    this.camera = camera;
    this.raycaster = new THREE.Raycaster();
    this.mouse = new THREE.Vector2();
    this.tools = new Map();

    // Initialize brush configuration
    this.brushConfig = {
      size: 10,
      strength: 0.5,
      falloff: 'gaussian'
    };

    // Create tools
    this.initTools();
    
    // Set default tool
    this.setTool('fill');

    // Initialize event listeners
    this.initEventListeners();
  }

  private initTools(): void {
    this.tools.set('fill', new FillTool('Fill', this.terrainMesh, this.scene));
    this.tools.set('dig', new DigTool('Dig', this.terrainMesh, this.scene));
  }

  private initEventListeners(): void {
    // Mouse move for raycasting
    document.addEventListener('mousemove', (event) => {
      // Calculate normalized device coordinates
      this.mouse.x = (event.clientX / window.innerWidth) * 2 - 1;
      this.mouse.y = -(event.clientY / window.innerHeight) * 2 + 1;

      // Update raycaster
      this.updateIntersection();
    });

    // Mouse down to start editing
    document.addEventListener('mousedown', (event) => {
      if (event.button === 0 && this.intersectionPoint) { // Left click
        this.isEditing = true;
      }
    });

    // Mouse up to stop editing
    document.addEventListener('mouseup', (event) => {
      if (event.button === 0) {
        this.isEditing = false;
      }
    });

    // Update brush config from UI
    const brushSizeSlider = document.getElementById('brush-size') as HTMLInputElement;
    if (brushSizeSlider) {
      brushSizeSlider.addEventListener('input', () => {
        this.brushConfig.size = parseFloat(brushSizeSlider.value);
      });
    }

    const brushStrengthSlider = document.getElementById('brush-strength') as HTMLInputElement;
    if (brushStrengthSlider) {
      brushStrengthSlider.addEventListener('input', () => {
        this.brushConfig.strength = parseFloat(brushStrengthSlider.value);
      });
    }

    // Tool selection buttons
    document.getElementById('fill-tool')?.addEventListener('click', () => {
      this.setTool('fill');
      this.updateToolButtons('fill');
    });

    document.getElementById('dig-tool')?.addEventListener('click', () => {
      this.setTool('dig');
      this.updateToolButtons('dig');
    });
  }

  private updateToolButtons(activeTool: string): void {
    // Remove active class from all buttons
    document.querySelectorAll('.tool-btn').forEach(btn => {
      btn.classList.remove('active');
    });

    // Add active class to selected button
    document.getElementById(`${activeTool}-tool`)?.classList.add('active');
  }

  private updateIntersection(): void {
    // Update the raycaster with camera and mouse position
    this.raycaster.setFromCamera(this.mouse, this.camera);

    // Check for intersection with terrain
    const intersects = this.raycaster.intersectObject(this.terrainMesh.mesh);

    if (intersects.length > 0) {
      this.intersectionPoint = intersects[0].point;
      
      // Update brush preview position
      if (this.currentTool && 'updateBrushPreview' in this.currentTool) {
        (this.currentTool as any).updateBrushPreview(this.intersectionPoint, this.brushConfig);
      }
    } else {
      this.intersectionPoint = null;
    }
  }

  setTool(toolType: ToolType): void {
    // Deactivate current tool
    if (this.currentTool) {
      this.currentTool.onDeactivate();
    }

    // Activate new tool
    const tool = this.tools.get(toolType);
    if (tool) {
      this.currentTool = tool;
      this.currentTool.onActivate();
    }
  }

  update(deltaTime: number): void {
    // Apply tool if editing and have intersection
    if (this.isEditing && this.currentTool && this.intersectionPoint) {
      this.currentTool.apply(this.intersectionPoint, this.brushConfig, deltaTime);
    }

    // Update intersection for brush preview
    this.updateIntersection();
  }

  getBrushConfig(): BrushConfig {
    return { ...this.brushConfig };
  }

  setBrushConfig(config: Partial<BrushConfig>): void {
    this.brushConfig = { ...this.brushConfig, ...config };
  }

  dispose(): void {
    // Deactivate current tool
    if (this.currentTool) {
      this.currentTool.onDeactivate();
    }

    // Clear tools
    this.tools.clear();
  }
} 