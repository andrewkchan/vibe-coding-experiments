import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls';
import Stats from 'stats.js';

export class SceneManager {
  private scene: THREE.Scene;
  private camera: THREE.PerspectiveCamera;
  private renderer: THREE.WebGLRenderer;
  private controls: OrbitControls;
  private stats: Stats;
  
  constructor(canvas: HTMLCanvasElement) {
    // Initialize scene
    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(0x87CEEB); // Sky blue for cartoon look
    this.scene.fog = new THREE.Fog(0x87CEEB, 50, 300);
    
    // Initialize camera
    const aspect = window.innerWidth / window.innerHeight;
    this.camera = new THREE.PerspectiveCamera(60, aspect, 0.1, 1000);
    this.camera.position.set(20, 25, 40);
    this.camera.lookAt(0, 0, 0);
    
    // Initialize renderer
    this.renderer = new THREE.WebGLRenderer({ 
      canvas, 
      antialias: true,
      alpha: true 
    });
    this.renderer.setSize(window.innerWidth, window.innerHeight);
    this.renderer.setPixelRatio(window.devicePixelRatio);
    this.renderer.shadowMap.enabled = true;
    this.renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    this.renderer.toneMapping = THREE.ACESFilmicToneMapping;
    this.renderer.toneMappingExposure = 1.2;
    
    // Initialize controls
    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.enableDamping = true;
    this.controls.dampingFactor = 0.05;
    this.controls.maxPolarAngle = Math.PI * 0.45; // Limit rotation to not go below ground
    this.controls.minDistance = 10;
    this.controls.maxDistance = 100;
    this.controls.target.set(0, 0, 0);
    
    // Initialize stats
    this.stats = new Stats();
    this.stats.showPanel(0); // FPS panel
    document.body.appendChild(this.stats.dom);
    this.stats.dom.style.position = 'absolute';
    this.stats.dom.style.top = '20px';
    this.stats.dom.style.right = '20px';
    
    // Setup lights
    this.setupLights();
    
    // Setup ground
    this.setupGround();
    
    // Handle window resize
    window.addEventListener('resize', this.onWindowResize.bind(this));
  }
  
  private setupLights(): void {
    // Ambient light for overall brightness
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.6);
    this.scene.add(ambientLight);
    
    // Main directional light (sun)
    const directionalLight = new THREE.DirectionalLight(0xffffff, 0.8);
    directionalLight.position.set(20, 30, 10);
    directionalLight.castShadow = true;
    directionalLight.shadow.camera.left = -30;
    directionalLight.shadow.camera.right = 30;
    directionalLight.shadow.camera.top = 30;
    directionalLight.shadow.camera.bottom = -30;
    directionalLight.shadow.camera.near = 0.1;
    directionalLight.shadow.camera.far = 100;
    directionalLight.shadow.mapSize.width = 2048;
    directionalLight.shadow.mapSize.height = 2048;
    directionalLight.shadow.bias = -0.001;
    this.scene.add(directionalLight);
    
    // Hemisphere light for nice color variation
    const hemisphereLight = new THREE.HemisphereLight(0x87CEEB, 0x80C080, 0.3);
    this.scene.add(hemisphereLight);
  }
  
  private setupGround(): void {
    // Create checkered ground with cartoon style
    const groundSize = 100;
    const tileSize = 5;
    const tiles = groundSize / tileSize;
    
    // Create canvas for texture
    const canvas = document.createElement('canvas');
    canvas.width = 512;
    canvas.height = 512;
    const ctx = canvas.getContext('2d')!;
    
    const tilePixels = canvas.width / tiles;
    
    for (let i = 0; i < tiles; i++) {
      for (let j = 0; j < tiles; j++) {
        ctx.fillStyle = (i + j) % 2 === 0 ? '#90EE90' : '#98FB98'; // Light green checkerboard
        ctx.fillRect(i * tilePixels, j * tilePixels, tilePixels, tilePixels);
      }
    }
    
    const texture = new THREE.CanvasTexture(canvas);
    texture.wrapS = THREE.RepeatWrapping;
    texture.wrapT = THREE.RepeatWrapping;
    
    const groundGeometry = new THREE.PlaneGeometry(groundSize, groundSize);
    const groundMaterial = new THREE.MeshPhongMaterial({ 
      map: texture,
      side: THREE.DoubleSide,
      shininess: 10
    });
    
    const ground = new THREE.Mesh(groundGeometry, groundMaterial);
    ground.rotation.x = -Math.PI / 2;
    ground.receiveShadow = true;
    this.scene.add(ground);
    
    // Add grid helper for better depth perception
    const gridHelper = new THREE.GridHelper(groundSize, tiles, 0x4a4a4a, 0x4a4a4a);
    gridHelper.material.opacity = 0.2;
    gridHelper.material.transparent = true;
    gridHelper.position.y = 0.01; // Slightly above ground to prevent z-fighting
    this.scene.add(gridHelper);
  }
  
  private onWindowResize(): void {
    const aspect = window.innerWidth / window.innerHeight;
    this.camera.aspect = aspect;
    this.camera.updateProjectionMatrix();
    this.renderer.setSize(window.innerWidth, window.innerHeight);
  }
  
  public update(): void {
    this.stats.begin();
    this.controls.update();
    this.renderer.render(this.scene, this.camera);
    this.stats.end();
  }
  
  public getScene(): THREE.Scene {
    return this.scene;
  }
  
  public getCamera(): THREE.Camera {
    return this.camera;
  }
  
  public getRenderer(): THREE.WebGLRenderer {
    return this.renderer;
  }
  
  public addObject(object: THREE.Object3D): void {
    this.scene.add(object);
  }
  
  public removeObject(object: THREE.Object3D): void {
    this.scene.remove(object);
  }
  
  public setOrbitControlsEnabled(enabled: boolean): void {
    this.controls.enabled = enabled;
    
    // Update cursor when dragging in camera mode
    if (enabled) {
      this.renderer.domElement.style.cursor = 'grab';
    }
  }
} 