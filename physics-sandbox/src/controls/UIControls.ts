import type { ObjectType, ControlMode } from './MouseControls';
import { MouseControls } from './MouseControls';
import { PhysicsWorld } from '../physics/PhysicsWorld';
import * as THREE from 'three';
import * as CANNON from 'cannon-es';

export class UIControls {
  private container: HTMLElement;
  private mouseControls: MouseControls;
  private physicsWorld: PhysicsWorld;
  private modeIndicator?: HTMLElement;
  
  constructor(
    container: HTMLElement,
    mouseControls: MouseControls,
    physicsWorld: PhysicsWorld
  ) {
    this.container = container;
    this.mouseControls = mouseControls;
    this.physicsWorld = physicsWorld;
    
    this.createUI();
    this.setupModeListener();
  }
  
  private createUI(): void {
    // Create mode switcher
    const modeSwitcher = this.createModeSwitcher();
    this.container.appendChild(modeSwitcher);
    
    // Create controls panel
    const controlsPanel = this.createControlsPanel();
    this.container.appendChild(controlsPanel);
    
    // Create object toolbar
    const toolbar = this.createObjectToolbar();
    this.container.appendChild(toolbar);
    
    // Create info text
    const infoText = this.createInfoText();
    this.container.appendChild(infoText);
  }
  
  private createModeSwitcher(): HTMLElement {
    const switcher = document.createElement('div');
    switcher.className = 'ui-panel mode-switcher';
    switcher.id = 'mode-switcher';
    
    switcher.innerHTML = `
      <div class="mode-buttons">
        <button class="mode-button active" data-mode="camera" title="Camera Mode (C)">
          <span class="mode-icon">📷</span>
          <span class="mode-label">Camera</span>
        </button>
        <button class="mode-button" data-mode="interact" title="Interact Mode (I)">
          <span class="mode-icon">✋</span>
          <span class="mode-label">Interact</span>
        </button>
      </div>
      <div class="mode-indicator" id="mode-indicator">
        <span class="mode-status">Camera Mode</span>
      </div>
    `;
    
    // Store reference to mode indicator
    this.modeIndicator = switcher.querySelector('#mode-indicator') as HTMLElement;
    
    // Add click handlers
    switcher.querySelectorAll('.mode-button').forEach(button => {
      button.addEventListener('click', () => {
        const mode = button.getAttribute('data-mode') as ControlMode;
        this.mouseControls.setMode(mode);
      });
    });
    
    return switcher;
  }
  
  private setupModeListener(): void {
    window.addEventListener('modeChanged', (event: Event) => {
      const customEvent = event as CustomEvent;
      const mode = customEvent.detail.mode as ControlMode;
      this.updateModeUI(mode);
    });
  }
  
  private updateModeUI(mode: ControlMode): void {
    // Update buttons
    const buttons = document.querySelectorAll('.mode-button');
    buttons.forEach(button => {
      if (button.getAttribute('data-mode') === mode) {
        button.classList.add('active');
      } else {
        button.classList.remove('active');
      }
    });
    
    // Update indicator
    if (this.modeIndicator) {
      const statusText = mode === 'camera' ? 'Camera Mode' : 'Interact Mode';
      this.modeIndicator.querySelector('.mode-status')!.textContent = statusText;
      this.modeIndicator.className = `mode-indicator ${mode}-mode`;
    }
    
    // Show/hide object toolbar based on mode
    const toolbar = document.getElementById('object-toolbar');
    if (toolbar) {
      toolbar.style.display = mode === 'interact' ? 'flex' : 'none';
    }
  }
  
  private createControlsPanel(): HTMLElement {
    const panel = document.createElement('div');
    panel.className = 'ui-panel';
    panel.id = 'controls-panel';
    
    panel.innerHTML = `
      <h3>Physics Controls</h3>
      
      <div class="control-group">
        <label>Gravity<span class="control-value" id="gravity-value">-9.82</span></label>
        <input type="range" id="gravity-slider" min="-20" max="20" value="-9.82" step="0.1">
      </div>
      
      <div class="control-group">
        <label>Time Scale<span class="control-value" id="timescale-value">1.0</span></label>
        <input type="range" id="timescale-slider" min="0" max="2" value="1" step="0.1">
      </div>
      
      <div class="control-group">
        <label>Wind Force<span class="control-value" id="wind-value">0</span></label>
        <input type="range" id="wind-slider" min="-10" max="10" value="0" step="0.5">
      </div>
      
      <button class="button" id="reset-button">Reset Scene</button>
      <button class="button danger" id="clear-button">Clear All</button>
    `;
    
    // Add event listeners after DOM is ready
    setTimeout(() => {
      this.setupControlListeners(panel);
    }, 0);
    
    return panel;
  }
  
  private createObjectToolbar(): HTMLElement {
    const toolbar = document.createElement('div');
    toolbar.className = 'ui-panel';
    toolbar.id = 'object-toolbar';
    
    const objects: { type: ObjectType; icon: string; title: string }[] = [
      { type: 'ball', icon: '⚪', title: 'Ball' },
      { type: 'box', icon: '◼️', title: 'Box' },
      { type: 'cylinder', icon: '⬭', title: 'Cylinder' },
      { type: 'cone', icon: '△', title: 'Cone' },
      { type: 'cloth', icon: '🏴', title: 'Cloth' }
    ];
    
    objects.forEach(obj => {
      const button = document.createElement('button');
      button.className = 'tool-button';
      button.innerHTML = obj.icon;
      button.title = obj.title;
      button.dataset.type = obj.type;
      
      if (obj.type === 'ball') {
        button.classList.add('active');
      }
      
      button.addEventListener('click', () => {
        this.setActiveObject(obj.type);
        
        // Update active state
        toolbar.querySelectorAll('.tool-button').forEach(btn => {
          btn.classList.remove('active');
        });
        button.classList.add('active');
      });
      
      toolbar.appendChild(button);
    });
    
    return toolbar;
  }
  
  private createInfoText(): HTMLElement {
    const info = document.createElement('div');
    info.className = 'info-text';
    info.innerHTML = `
      <strong>Mode Controls:</strong> Press <kbd>C</kbd> for Camera | <kbd>I</kbd> for Interact | <kbd>ESC</kbd> to exit<br>
      <strong>Camera Mode:</strong> Click & Drag to rotate | Scroll to zoom | Right-click & Drag to pan<br>
      <strong>Interact Mode:</strong> Left Click - Spawn | Shift+Drag - Move | Right Click - Delete
    `;
    return info;
  }
  
  private setupControlListeners(panel: HTMLElement): void {
    // Gravity control
    const gravitySlider = panel.querySelector('#gravity-slider') as HTMLInputElement;
    const gravityValue = panel.querySelector('#gravity-value') as HTMLSpanElement;
    
    gravitySlider?.addEventListener('input', (e) => {
      const value = parseFloat((e.target as HTMLInputElement).value);
      gravityValue.textContent = value.toFixed(2);
      this.physicsWorld.setGravity(0, value, 0);
    });
    
    // Time scale control
    const timescaleSlider = panel.querySelector('#timescale-slider') as HTMLInputElement;
    const timescaleValue = panel.querySelector('#timescale-value') as HTMLSpanElement;
    
    timescaleSlider?.addEventListener('input', (e) => {
      const value = parseFloat((e.target as HTMLInputElement).value);
      timescaleValue.textContent = value.toFixed(1);
      // Store timescale for use in animation loop
      (window as any).physicsTimeScale = value;
    });
    
    // Wind control
    const windSlider = panel.querySelector('#wind-slider') as HTMLInputElement;
    const windValue = panel.querySelector('#wind-value') as HTMLSpanElement;
    
    windSlider?.addEventListener('input', (e) => {
      const value = parseFloat((e.target as HTMLInputElement).value);
      windValue.textContent = value.toFixed(1);
      // Store wind value for use in animation loop
      (window as any).windForce = value;
    });
    
    // Reset button
    const resetButton = panel.querySelector('#reset-button') as HTMLButtonElement;
    resetButton?.addEventListener('click', () => {
      this.physicsWorld.reset();
      // Reset sliders
      gravitySlider.value = '-9.82';
      gravityValue.textContent = '-9.82';
      timescaleSlider.value = '1';
      timescaleValue.textContent = '1.0';
      windSlider.value = '0';
      windValue.textContent = '0';
      (window as any).physicsTimeScale = 1;
      (window as any).windForce = 0;
    });
    
    // Clear button
    const clearButton = panel.querySelector('#clear-button') as HTMLButtonElement;
    clearButton?.addEventListener('click', () => {
      if (confirm('Clear all objects from the scene?')) {
        this.clearScene();
      }
    });
  }
  
  private setActiveObject(type: ObjectType): void {
    this.mouseControls.setObjectType(type);
  }
  
  private clearScene(): void {
    // Get all bodies except ground
    const bodiesToRemove: string[] = [];
    this.physicsWorld.getBodies().forEach((body, id) => {
      if (id !== 'ground') {
        bodiesToRemove.push(id);
      }
    });
    
    // Remove all bodies
    bodiesToRemove.forEach(id => {
      const mesh = (this.physicsWorld as any).meshes.get(id);
      if (mesh) {
        (mesh.parent as THREE.Scene)?.remove(mesh);
      }
      this.physicsWorld.removeBody(id);
    });
    
    // Also remove any cloth objects
    (window as any).clothObjects?.forEach((cloth: any) => {
      cloth.getParticles().forEach((particle: CANNON.Body) => {
        this.physicsWorld.getWorld().removeBody(particle);
      });
      cloth.getConstraints().forEach((constraint: CANNON.Constraint) => {
        this.physicsWorld.getWorld().removeConstraint(constraint);
      });
      if (cloth.mesh.parent) {
        cloth.mesh.parent.remove(cloth.mesh);
      }
    });
    
    (window as any).clothObjects = [];
  }
} 