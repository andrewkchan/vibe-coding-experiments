import * as THREE from 'three';
import { TerrainGenerator } from './terrain/TerrainGenerator';
import { TerrainMesh } from './terrain/TerrainMesh';
import { HeightmapEditor } from './terrain/HeightmapEditor';
import { FirstPersonCamera } from './camera/FirstPersonCamera';
import { TerrainConfig, NoiseConfig } from './types';

class TerrainEditor {
  private scene!: THREE.Scene;
  private camera!: THREE.PerspectiveCamera;
  private renderer!: THREE.WebGLRenderer;
  private terrainMesh!: TerrainMesh;
  private terrainGenerator!: TerrainGenerator;
  private heightmapEditor!: HeightmapEditor;
  private fpsCamera!: FirstPersonCamera;
  private frameCount: number = 0;
  private lastTime: number = 0;
  private clock: THREE.Clock;

  constructor() {
    this.clock = new THREE.Clock();
    this.initScene();
    this.initTerrain();
    this.initCamera();
    this.initEditor();
    this.initUI();
    this.animate();
    
    // Handle window resize
    window.addEventListener('resize', () => this.onWindowResize());
  }

  private initScene(): void {
    // Create scene
    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(0x101020);
    this.scene.fog = new THREE.Fog(0x101020, 100, 500);

    // Create camera
    const aspect = window.innerWidth / window.innerHeight;
    this.camera = new THREE.PerspectiveCamera(75, aspect, 0.1, 1000);

    // Create renderer
    const canvas = document.getElementById('canvas') as HTMLCanvasElement;
    this.renderer = new THREE.WebGLRenderer({ 
      canvas,
      antialias: true 
    });
    this.renderer.setSize(window.innerWidth, window.innerHeight);
    this.renderer.setPixelRatio(window.devicePixelRatio);

    // Add ambient light
    const ambientLight = new THREE.AmbientLight(0x404040);
    this.scene.add(ambientLight);

    // Add directional light
    const directionalLight = new THREE.DirectionalLight(0xffffff, 0.5);
    directionalLight.position.set(1, 1, 0.5);
    this.scene.add(directionalLight);

    // Add grid helper for reference
    const gridHelper = new THREE.GridHelper(200, 20, 0x404040, 0x202020);
    gridHelper.position.y = -0.1;
    this.scene.add(gridHelper);
  }

  private initCamera(): void {
    // Initialize first-person camera
    this.fpsCamera = new FirstPersonCamera(this.camera, {
      moveSpeed: 50,
      lookSpeed: 0.002,
      verticalSpeed: 30,
      acceleration: 200,
      friction: 10
    });

    // Set initial position above terrain
    this.fpsCamera.setPosition(new THREE.Vector3(0, 50, 100));

    // Add camera to scene
    this.scene.add(this.fpsCamera.getObject());
  }

  private initTerrain(): void {
    // Terrain configuration
    const terrainConfig: TerrainConfig = {
      width: 200,
      height: 200,
      widthSegments: 128,
      heightSegments: 128,
      maxHeight: 30,
      minHeight: -5
    };

    // Noise configuration
    const noiseConfig: NoiseConfig = {
      scale: 3,
      octaves: 4,
      persistence: 0.5,
      lacunarity: 2,
      seed: Math.random() * 1000
    };

    // Create terrain generator and mesh
    this.terrainGenerator = new TerrainGenerator(noiseConfig.seed);
    this.terrainMesh = new TerrainMesh(terrainConfig);

    // Generate initial heightmap
    const heightmap = this.terrainGenerator.generateHeightmap(terrainConfig, noiseConfig);
    this.terrainMesh.updateHeightmap(heightmap);

    // Add terrain to scene
    this.scene.add(this.terrainMesh.mesh);
  }

  private initEditor(): void {
    // Initialize the heightmap editor
    this.heightmapEditor = new HeightmapEditor(
      this.terrainMesh,
      this.scene,
      this.camera
    );
  }

  private initUI(): void {
    // Update FPS display
    setInterval(() => {
      const fps = Math.round(this.frameCount);
      document.getElementById('fps')!.textContent = `FPS: ${fps}`;
      this.frameCount = 0;
    }, 1000);

    // Update position display
    setInterval(() => {
      const pos = this.fpsCamera.getPosition();
      document.getElementById('position')!.textContent = 
        `Position: (${pos.x.toFixed(1)}, ${pos.y.toFixed(1)}, ${pos.z.toFixed(1)})`;
    }, 100);

    // Reset terrain button
    document.getElementById('reset-terrain')!.addEventListener('click', () => {
      this.resetTerrain();
    });

    // Brush size slider
    const brushSizeSlider = document.getElementById('brush-size') as HTMLInputElement;
    const brushSizeValue = document.getElementById('brush-size-value')!;
    brushSizeSlider.addEventListener('input', () => {
      brushSizeValue.textContent = brushSizeSlider.value;
    });

    // Brush strength slider
    const brushStrengthSlider = document.getElementById('brush-strength') as HTMLInputElement;
    const brushStrengthValue = document.getElementById('brush-strength-value')!;
    brushStrengthSlider.addEventListener('input', () => {
      brushStrengthValue.textContent = brushStrengthSlider.value;
    });
  }

  private resetTerrain(): void {
    const config = this.terrainMesh.getConfig();
    const noiseConfig: NoiseConfig = {
      scale: 3,
      octaves: 4,
      persistence: 0.5,
      lacunarity: 2,
      seed: Math.random() * 1000
    };

    this.terrainGenerator = new TerrainGenerator(noiseConfig.seed);
    const heightmap = this.terrainGenerator.generateHeightmap(config, noiseConfig);
    this.terrainMesh.updateHeightmap(heightmap);
  }

  private animate = (): void => {
    requestAnimationFrame(this.animate);
    
    // Calculate delta time
    const deltaTime = this.clock.getDelta();
    
    // Update frame count for FPS
    this.frameCount++;

    // Update camera
    this.fpsCamera.update(deltaTime);

    // Update heightmap editor
    this.heightmapEditor.update(deltaTime);

    // Render
    this.renderer.render(this.scene, this.camera);
  }

  private onWindowResize(): void {
    const width = window.innerWidth;
    const height = window.innerHeight;

    this.camera.aspect = width / height;
    this.camera.updateProjectionMatrix();

    this.renderer.setSize(width, height);
  }
}

// Initialize the editor when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
  new TerrainEditor();
}); 