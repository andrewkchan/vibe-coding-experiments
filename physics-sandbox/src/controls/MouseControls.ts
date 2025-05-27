import * as THREE from 'three';
import * as CANNON from 'cannon-es';
import { PhysicsWorld } from '../physics/PhysicsWorld';
import { Ball } from '../physics/objects/Ball';
import { RigidBody } from '../physics/objects/RigidBody';
import { Cloth } from '../physics/objects/Cloth';

export type ObjectType = 'ball' | 'box' | 'cylinder' | 'cone' | 'cloth';
export type ControlMode = 'camera' | 'interact';

export class MouseControls {
  private camera: THREE.Camera;
  private domElement: HTMLElement;
  private raycaster: THREE.Raycaster;
  private mouse: THREE.Vector2;
  private selectedObject: CANNON.Body | null = null;
  private selectedConstraint: CANNON.PointToPointConstraint | null = null;
  private selectedMesh: THREE.Mesh | null = null;
  private originalMaterial: THREE.Material | null = null;
  private hoveredMesh: THREE.Mesh | null = null;
  private hoveredMaterial: THREE.Material | null = null;
  private currentObjectType: ObjectType = 'ball';
  private currentMode: ControlMode = 'camera';
  private scene: THREE.Scene;
  private physicsWorld: PhysicsWorld;
  private objectIdCounter: number = 0;
  
  constructor(
    camera: THREE.Camera,
    domElement: HTMLElement,
    scene: THREE.Scene,
    physicsWorld: PhysicsWorld
  ) {
    this.camera = camera;
    this.domElement = domElement;
    this.scene = scene;
    this.physicsWorld = physicsWorld;
    this.raycaster = new THREE.Raycaster();
    this.mouse = new THREE.Vector2();
    
    this.setupEventListeners();
  }
  
  private setupEventListeners(): void {
    this.domElement.addEventListener('mousedown', this.onMouseDown.bind(this));
    this.domElement.addEventListener('mousemove', this.onMouseMove.bind(this));
    this.domElement.addEventListener('mouseup', this.onMouseUp.bind(this));
    this.domElement.addEventListener('contextmenu', (e) => e.preventDefault());
    
    // Add keyboard shortcuts for mode switching
    window.addEventListener('keydown', this.onKeyDown.bind(this));
    window.addEventListener('keyup', this.onKeyUp.bind(this));
  }
  
  private onKeyDown(event: KeyboardEvent): void {
    // Update body class when shift is held
    if (event.key === 'Shift' && this.currentMode === 'interact') {
      document.body.classList.add('shift-held');
    }
    
    // Press 'C' for camera mode
    if (event.key.toLowerCase() === 'c') {
      this.setMode('camera');
    }
    // Press 'I' for interact mode
    else if (event.key.toLowerCase() === 'i') {
      this.setMode('interact');
    }
    // Press 'Escape' to quickly switch to camera mode
    else if (event.key === 'Escape') {
      this.setMode('camera');
      this.stopDragging();
    }
  }
  
  private onKeyUp(event: KeyboardEvent): void {
    // Remove shift class when released
    if (event.key === 'Shift') {
      document.body.classList.remove('shift-held');
      this.clearHover();
    }
  }
  
  private onMouseDown(event: MouseEvent): void {
    // Only handle interactions in interact mode
    if (this.currentMode !== 'interact') return;
    
    if (event.button === 0) { // Left click
      this.updateMouse(event);
      
      // Check if we're clicking on an existing object
      const intersects = this.getIntersects();
      
      if (intersects.length > 0 && event.shiftKey) {
        // Drag mode if shift is held
        this.startDragging(intersects[0]);
      } else if (!event.shiftKey) {
        // Otherwise spawn new object
        this.spawnObject();
      }
    } else if (event.button === 2) { // Right click
      // Delete object under cursor
      this.deleteObjectUnderCursor(event);
    }
  }
  
  private onMouseMove(event: MouseEvent): void {
    this.updateMouse(event);
    
    // Only handle dragging and hovering in interact mode
    if (this.currentMode !== 'interact') {
      this.clearHover();
      return;
    }
    
    // Handle hover effect when not dragging
    if (!this.selectedObject && event.shiftKey) {
      const intersects = this.getIntersects();
      if (intersects.length > 0) {
        const mesh = intersects[0].object as THREE.Mesh;
        
        // Check if it's a draggable object
        let isDraggable = false;
        this.physicsWorld.getBodies().forEach((body, id) => {
          const associatedMesh = (this.physicsWorld as any).meshes.get(id);
          if (associatedMesh === mesh && body.mass > 0) {
            isDraggable = true;
          }
        });
        
        if (isDraggable && mesh !== this.hoveredMesh) {
          this.clearHover();
          this.hoveredMesh = mesh;
          this.hoveredMaterial = mesh.material as THREE.Material;
          
          // Apply hover highlight
          const hoverMaterial = (mesh.material as THREE.MeshPhongMaterial).clone();
          hoverMaterial.emissive = new THREE.Color(0x00ff00);
          hoverMaterial.emissiveIntensity = 0.2;
          mesh.material = hoverMaterial;
        }
      } else {
        this.clearHover();
      }
    } else if (!event.shiftKey) {
      this.clearHover();
    }
    
    if (this.selectedObject && this.selectedConstraint) {
      // Get intersection with an invisible plane at the object's depth
      const cameraDirection = new THREE.Vector3();
      this.camera.getWorldDirection(cameraDirection);
      
      // Create a plane at the object's position facing the camera
      const objectPos = new THREE.Vector3(
        this.selectedObject.position.x,
        this.selectedObject.position.y,
        this.selectedObject.position.z
      );
      const plane = new THREE.Plane(cameraDirection.negate(), -cameraDirection.dot(objectPos));
      
      // Cast ray and find intersection with plane
      this.raycaster.setFromCamera(this.mouse, this.camera);
      const intersection = new THREE.Vector3();
      this.raycaster.ray.intersectPlane(plane, intersection);
      
      if (intersection) {
        // Update the constraint anchor point smoothly
        const constraintBody = (this.selectedConstraint as any).constraintBody;
        if (constraintBody) {
          // Smoothly move the constraint body
          const currentPos = constraintBody.position;
          const targetPos = intersection;
          
          // Use lerping for smoother movement
          const lerpFactor = 0.3;
          constraintBody.position.x += (targetPos.x - currentPos.x) * lerpFactor;
          constraintBody.position.y += (targetPos.y - currentPos.y) * lerpFactor;
          constraintBody.position.z += (targetPos.z - currentPos.z) * lerpFactor;
          
          // Wake up the selected body
          this.selectedObject.wakeUp();
        }
      }
    }
  }
  
  private onMouseUp(event: MouseEvent): void {
    if (event.button === 0 && this.currentMode === 'interact') {
      this.stopDragging();
    }
  }
  
  private updateMouse(event: MouseEvent): void {
    const rect = this.domElement.getBoundingClientRect();
    this.mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    this.mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
  }
  
  private getIntersects(): THREE.Intersection[] {
    this.raycaster.setFromCamera(this.mouse, this.camera);
    
    // Get all meshes in the scene
    const meshes: THREE.Mesh[] = [];
    this.scene.traverse((child) => {
      if (child instanceof THREE.Mesh && child !== this.scene.getObjectByName('ground')) {
        meshes.push(child);
      }
    });
    
    return this.raycaster.intersectObjects(meshes);
  }
  
  private getWorldPosition(distance: number): THREE.Vector3 | null {
    this.raycaster.setFromCamera(this.mouse, this.camera);
    const direction = this.raycaster.ray.direction;
    const origin = this.raycaster.ray.origin;
    
    return new THREE.Vector3(
      origin.x + direction.x * distance,
      origin.y + direction.y * distance,
      origin.z + direction.z * distance
    );
  }
  
  private startDragging(intersection: THREE.Intersection): void {
    const mesh = intersection.object as THREE.Mesh;
    
    // Find the physics body associated with this mesh
    let foundBody: CANNON.Body | null = null;
    let foundId: string | null = null;
    
    // Use a more precise way to find the correct body
    this.physicsWorld.getBodies().forEach((body, id) => {
      const associatedMesh = (this.physicsWorld as any).meshes.get(id);
      if (associatedMesh === mesh && body.mass > 0) {
        foundBody = body;
        foundId = id;
      }
    });
    
    if (!foundBody || !foundId) return;
    
    this.selectedObject = foundBody;
    this.selectedMesh = mesh;
    
    // Store original material and apply highlight
    this.originalMaterial = mesh.material as THREE.Material;
    const highlightMaterial = (mesh.material as THREE.MeshPhongMaterial).clone();
    highlightMaterial.emissive = new THREE.Color(0xffff00);
    highlightMaterial.emissiveIntensity = 0.3;
    mesh.material = highlightMaterial;
    
    // Create a more stable constraint
    const worldPos = intersection.point;
    const localPoint = (foundBody as any).pointToLocalFrame(
      new CANNON.Vec3(worldPos.x, worldPos.y, worldPos.z)
    );
    
    // Create a static body to attach the constraint to
    const constraintBody = new CANNON.Body({ 
      mass: 0,
      type: CANNON.Body.STATIC,
      position: new CANNON.Vec3(worldPos.x, worldPos.y, worldPos.z)
    });
    this.physicsWorld.getWorld().addBody(constraintBody);
    
    // Create point-to-point constraint with softer settings
    this.selectedConstraint = new CANNON.PointToPointConstraint(
      foundBody,
      localPoint,
      constraintBody,
      new CANNON.Vec3(0, 0, 0)
    );
    
    // Store the constraint body for cleanup
    (this.selectedConstraint as any).constraintBody = constraintBody;
    
    // Add constraint to world
    this.physicsWorld.getWorld().addConstraint(this.selectedConstraint);
    
    // Reduce velocity damping on the selected body for smoother movement
    (this.selectedObject as any).originalLinearDamping = (this.selectedObject as any).linearDamping;
    (this.selectedObject as any).originalAngularDamping = (this.selectedObject as any).angularDamping;
    (this.selectedObject as any).linearDamping = 0.4;
    (this.selectedObject as any).angularDamping = 0.4;
  }
  
  private stopDragging(): void {
    // Clear hover when starting to drag
    this.clearHover();
    
    if (this.selectedConstraint) {
      this.physicsWorld.getWorld().removeConstraint(this.selectedConstraint);
      
      // Remove the constraint body
      const constraintBody = (this.selectedConstraint as any).constraintBody;
      if (constraintBody) {
        this.physicsWorld.getWorld().removeBody(constraintBody);
      }
      
      // Restore original damping
      if (this.selectedObject) {
        this.selectedObject.linearDamping = (this.selectedObject as any).originalLinearDamping || 0.01;
        this.selectedObject.angularDamping = (this.selectedObject as any).originalAngularDamping || 0.01;
      }
      
      this.selectedConstraint = null;
    }
    
    // Restore original material
    if (this.selectedMesh && this.originalMaterial) {
      this.selectedMesh.material = this.originalMaterial;
      this.selectedMesh = null;
      this.originalMaterial = null;
    }
    
    this.selectedObject = null;
  }
  
  private spawnObject(): void {
    const worldPos = this.getWorldPosition(15);
    if (!worldPos) return;
    
    const id = `object_${this.objectIdCounter++}`;
    
    switch (this.currentObjectType) {
      case 'ball':
        this.spawnBall(worldPos, id);
        break;
      case 'box':
      case 'cylinder':
      case 'cone':
        this.spawnRigidBody(worldPos, id, this.currentObjectType);
        break;
      case 'cloth':
        this.spawnCloth(worldPos, id);
        break;
    }
  }
  
  private spawnBall(position: THREE.Vector3, id: string): void {
    const ball = new Ball({
      radius: Math.random() * 0.5 + 0.5,
      color: Ball.getRandomColor(),
      mass: 1,
      position,
      velocity: new THREE.Vector3(
        (Math.random() - 0.5) * 5,
        0,
        (Math.random() - 0.5) * 5
      )
    });
    
    this.scene.add(ball.mesh);
    this.physicsWorld.addBody(id, ball.body, ball.mesh);
  }
  
  private spawnRigidBody(position: THREE.Vector3, id: string, shape: 'box' | 'cylinder' | 'cone'): void {
    const size = new THREE.Vector3(
      Math.random() * 2 + 1,
      Math.random() * 2 + 1,
      Math.random() * 2 + 1
    );
    
    const rigidBody = new RigidBody({
      shape,
      size,
      color: RigidBody.getRandomColor(),
      mass: size.x * size.y * size.z * 0.5,
      position,
      rotation: new THREE.Euler(
        Math.random() * Math.PI,
        Math.random() * Math.PI,
        Math.random() * Math.PI
      )
    });
    
    this.scene.add(rigidBody.mesh);
    this.physicsWorld.addBody(id, rigidBody.body, rigidBody.mesh);
  }
  
  private spawnCloth(position: THREE.Vector3, id: string): void {
    const cloth = new Cloth({
      width: 8,
      height: 8,
      segments: 12,
      color: 0xe74c3c,
      position
    });
    
    this.scene.add(cloth.mesh);
    
    // Add all cloth particles to physics world
    cloth.getParticles().forEach((particle, index) => {
      this.physicsWorld.getWorld().addBody(particle);
    });
    
    // Add constraints
    cloth.getConstraints().forEach(constraint => {
      this.physicsWorld.getWorld().addConstraint(constraint);
    });
    
    // Store cloth reference for updates
    (cloth.mesh as any).clothInstance = cloth;
  }
  
  private deleteObjectUnderCursor(event: MouseEvent): void {
    this.updateMouse(event);
    const intersects = this.getIntersects();
    
    if (intersects.length > 0) {
      const mesh = intersects[0].object as THREE.Mesh;
      
      // Find and remove from physics world
      this.physicsWorld.getBodies().forEach((body, id) => {
        const associatedMesh = this.physicsWorld['meshes'].get(id);
        if (associatedMesh === mesh) {
          this.physicsWorld.removeBody(id);
          this.scene.remove(mesh);
        }
      });
      
      // Handle cloth deletion
      if ((mesh as any).clothInstance) {
        const cloth = (mesh as any).clothInstance as Cloth;
        cloth.getParticles().forEach(particle => {
          this.physicsWorld.getWorld().removeBody(particle);
        });
        cloth.getConstraints().forEach(constraint => {
          this.physicsWorld.getWorld().removeConstraint(constraint);
        });
        this.scene.remove(mesh);
      }
    }
  }
  
  public setObjectType(type: ObjectType): void {
    this.currentObjectType = type;
  }
  
  public getCurrentObjectType(): ObjectType {
    return this.currentObjectType;
  }
  
  public setMode(mode: ControlMode): void {
    this.currentMode = mode;
    
    // Clear hover when changing modes
    this.clearHover();
    
    // Update cursor style
    if (mode === 'camera') {
      this.domElement.style.cursor = 'grab';
    } else {
      this.domElement.style.cursor = 'crosshair';
    }
    
    // Update body class for visual styling
    document.body.className = `${mode}-mode`;
    
    // Dispatch custom event for UI updates
    window.dispatchEvent(new CustomEvent('modeChanged', { detail: { mode } }));
  }
  
  public getMode(): ControlMode {
    return this.currentMode;
  }
  
  private clearHover(): void {
    if (this.hoveredMesh && this.hoveredMaterial) {
      this.hoveredMesh.material = this.hoveredMaterial;
      this.hoveredMesh = null;
      this.hoveredMaterial = null;
    }
  }
} 