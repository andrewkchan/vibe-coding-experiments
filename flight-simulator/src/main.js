import * as THREE from 'three';
import { Cessna } from './aircraft/Cessna.js';
import { Terrain } from './world/Terrain.js';
import { Sky } from './world/Sky.js';
import { Airport } from './world/Airport.js';
import { InputManager } from './controls/InputManager.js';
import { CameraManager } from './controls/CameraManager.js';
import { HUD } from './ui/HUD.js';
import { FlightDynamics } from './physics/FlightDynamics.js';

class FlightSimulator {
    constructor() {
        this.scene = null;
        this.renderer = null;
        this.cessna = null;
        this.terrain = null;
        this.sky = null;
        this.airport = null;
        this.inputManager = null;
        this.cameraManager = null;
        this.hud = null;
        this.flightDynamics = null;
        this.clock = new THREE.Clock();
        
        this.init();
    }

    init() {
        // Setup Three.js
        this.setupRenderer();
        this.setupScene();
        
        // Create world
        this.terrain = new Terrain(this.scene);
        this.sky = new Sky(this.scene);
        this.airport = new Airport(this.scene);
        
        // Create aircraft
        this.cessna = new Cessna(this.scene);
        
        // Setup controls and camera
        this.inputManager = new InputManager();
        window.inputManager = this.inputManager;
        this.cameraManager = new CameraManager(this.renderer.domElement, this.cessna);
        
        // Setup physics
        this.flightDynamics = new FlightDynamics(this.cessna);
        
        // Setup UI
        this.hud = new HUD();
        
        // Hide loading screen
        setTimeout(() => {
            document.getElementById('loading').classList.add('hidden');
        }, 1000);
        
        // Start animation loop
        this.animate();
    }

    setupRenderer() {
        const canvas = document.getElementById('game-canvas');
        this.renderer = new THREE.WebGLRenderer({
            canvas,
            antialias: true,
            alpha: true
        });
        
        this.renderer.setSize(window.innerWidth, window.innerHeight);
        this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
        this.renderer.shadowMap.enabled = true;
        this.renderer.shadowMap.type = THREE.PCFSoftShadowMap;
        this.renderer.toneMapping = THREE.ACESFilmicToneMapping;
        this.renderer.toneMappingExposure = 1.2;
        
        // Handle window resize
        window.addEventListener('resize', () => {
            this.renderer.setSize(window.innerWidth, window.innerHeight);
            this.cameraManager.handleResize();
        });
    }

    setupScene() {
        this.scene = new THREE.Scene();
        this.scene.fog = new THREE.Fog(0x87CEEB, 100, 2000);
        
        // Ambient light for cartoon look
        const ambientLight = new THREE.AmbientLight(0xffffff, 0.6);
        this.scene.add(ambientLight);
        
        // Directional light (sun)
        const dirLight = new THREE.DirectionalLight(0xffffff, 0.8);
        dirLight.position.set(50, 100, 50);
        dirLight.castShadow = true;
        dirLight.shadow.camera.left = -100;
        dirLight.shadow.camera.right = 100;
        dirLight.shadow.camera.top = 100;
        dirLight.shadow.camera.bottom = -100;
        dirLight.shadow.camera.near = 0.1;
        dirLight.shadow.camera.far = 300;
        dirLight.shadow.mapSize.width = 2048;
        dirLight.shadow.mapSize.height = 2048;
        this.scene.add(dirLight);
        
        // Hemisphere light for nice ambient colors
        const hemiLight = new THREE.HemisphereLight(0x87CEEB, 0x98D982, 0.3);
        this.scene.add(hemiLight);
    }

    animate() {
        requestAnimationFrame(() => this.animate());
        
        const deltaTime = this.clock.getDelta();
        const elapsedTime = this.clock.getElapsedTime();
        
        // Update physics
        this.flightDynamics.update(deltaTime, this.inputManager.getInput());
        
        // Update camera
        this.cameraManager.update(deltaTime);
        
        // Update HUD
        this.hud.update(this.cessna);
        
        // Update animated elements
        this.sky.update(elapsedTime);
        
        // Render scene
        this.renderer.render(this.scene, this.cameraManager.getActiveCamera());
    }
}

// Start the simulator when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new FlightSimulator();
}); 