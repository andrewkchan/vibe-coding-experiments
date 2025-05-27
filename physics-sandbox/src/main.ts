import './style.css'
import { SceneManager } from './scene/SceneManager'
import { PhysicsWorld } from './physics/PhysicsWorld'
import { MouseControls } from './controls/MouseControls'
import { UIControls } from './controls/UIControls'
import * as THREE from 'three'

// Global variables for animation loop
declare global {
  interface Window {
    physicsTimeScale: number
    windForce: number
    clothObjects: any[]
  }
}

window.physicsTimeScale = 1
window.windForce = 0
window.clothObjects = []

class PhysicsSandbox {
  private sceneManager: SceneManager
  private physicsWorld: PhysicsWorld
  private mouseControls: MouseControls
  private uiControls?: UIControls
  private clock: THREE.Clock
  private lastTime: number = 0
  
  constructor() {
    // Get canvas element
    const canvas = document.getElementById('canvas') as HTMLCanvasElement
    if (!canvas) {
      throw new Error('Canvas element not found')
    }
    
    // Initialize managers
    this.sceneManager = new SceneManager(canvas)
    this.physicsWorld = new PhysicsWorld()
    this.clock = new THREE.Clock()
    
    // Initialize controls
    this.mouseControls = new MouseControls(
      this.sceneManager.getCamera(),
      canvas,
      this.sceneManager.getScene(),
      this.physicsWorld
    )
    
    const uiContainer = document.getElementById('ui-container')
    if (uiContainer) {
      this.uiControls = new UIControls(
        uiContainer,
        this.mouseControls,
        this.physicsWorld
      )
    }
    
    // Start animation loop
    this.animate()
    
    // Setup mode change listener
    this.setupModeListener()
    
    // Add some initial objects
    this.addInitialObjects()
  }
  
  private setupModeListener(): void {
    window.addEventListener('modeChanged', (event: Event) => {
      const customEvent = event as CustomEvent
      const mode = customEvent.detail.mode
      
      // Enable orbit controls only in camera mode
      this.sceneManager.setOrbitControlsEnabled(mode === 'camera')
    })
  }
  
  private addInitialObjects(): void {
    // Switch to interact mode first
    setTimeout(() => {
      this.mouseControls.setMode('interact')
      
      // Add a few balls
      for (let i = 0; i < 3; i++) {
        setTimeout(() => {
          const event = new MouseEvent('mousedown', {
            clientX: window.innerWidth / 2 + (i - 1) * 100,
            clientY: window.innerHeight / 3,
            button: 0
          })
          document.getElementById('canvas')?.dispatchEvent(event)
        }, i * 200 + 100)
      }
      
      // Switch back to camera mode after spawning
      setTimeout(() => {
        this.mouseControls.setMode('camera')
      }, 800)
    }, 500)
  }
  
  private animate = (): void => {
    requestAnimationFrame(this.animate)
    
    const currentTime = this.clock.getElapsedTime()
    const deltaTime = currentTime - this.lastTime
    this.lastTime = currentTime
    
    // Update physics with time scale
    const scaledDelta = Math.min(deltaTime * window.physicsTimeScale, 1/30)
    this.physicsWorld.update(scaledDelta)
    
    // Update cloth objects
    this.updateCloths()
    
    // Apply wind to cloth
    if (window.windForce !== 0) {
      this.applyWindToCloths()
    }
    
    // Update scene
    this.sceneManager.update()
  }
  
  private updateCloths(): void {
    this.sceneManager.getScene().traverse((child) => {
      if ((child as any).clothInstance) {
        const cloth = (child as any).clothInstance
        cloth.updateGeometry()
      }
    })
  }
  
  private applyWindToCloths(): void {
    const windVector = new THREE.Vector3(window.windForce, 0, window.windForce * 0.5)
    
    this.sceneManager.getScene().traverse((child) => {
      if ((child as any).clothInstance) {
        const cloth = (child as any).clothInstance
        cloth.applyWind(windVector)
      }
    })
  }
}

// Initialize the application
document.addEventListener('DOMContentLoaded', () => {
  new PhysicsSandbox()
})
