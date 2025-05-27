import * as THREE from 'three';
import { CameraConfig, InputState } from '../types';

export class FirstPersonCamera {
  private camera: THREE.PerspectiveCamera;
  private config: CameraConfig;
  private input: InputState;
  private velocity: THREE.Vector3;
  private euler: THREE.Euler;
  private pitchObject: THREE.Object3D;
  private yawObject: THREE.Object3D;
  private isPointerLocked: boolean = false;

  constructor(camera: THREE.PerspectiveCamera, config?: Partial<CameraConfig>) {
    this.camera = camera;
    this.config = {
      moveSpeed: 50,
      lookSpeed: 0.002,
      verticalSpeed: 30,
      acceleration: 200,
      friction: 10,
      ...config
    };

    this.input = {
      keys: new Set(),
      mouseDown: false,
      mousePosition: { x: 0, y: 0 },
      mouseDelta: { x: 0, y: 0 }
    };

    this.velocity = new THREE.Vector3();
    this.euler = new THREE.Euler(0, 0, 0, 'YXZ');

    // Create pitch and yaw objects for proper FPS camera rotation
    this.pitchObject = new THREE.Object3D();
    this.pitchObject.add(this.camera);

    this.yawObject = new THREE.Object3D();
    this.yawObject.add(this.pitchObject);

    // Set initial camera position
    this.yawObject.position.copy(camera.position);
    camera.position.set(0, 0, 0);

    this.initEventListeners();
  }

  private initEventListeners(): void {
    // Keyboard events
    document.addEventListener('keydown', (e) => {
      this.input.keys.add(e.key.toLowerCase());
    });

    document.addEventListener('keyup', (e) => {
      this.input.keys.delete(e.key.toLowerCase());
    });

    // Mouse events - use right click for camera control
    document.addEventListener('mousedown', (e) => {
      if (e.button === 2) { // Right click
        this.input.mouseDown = true;
        e.preventDefault(); // Prevent context menu
      }
    });

    document.addEventListener('mouseup', (e) => {
      if (e.button === 2) {
        this.input.mouseDown = false;
      }
    });

    document.addEventListener('mousemove', (e) => {
      if (this.input.mouseDown) {
        this.input.mouseDelta.x = e.movementX;
        this.input.mouseDelta.y = e.movementY;
      }
    });

    // Prevent right-click context menu
    document.addEventListener('contextmenu', (e) => {
      e.preventDefault();
    });

    // Exit pointer lock on ESC
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && this.isPointerLocked) {
        document.exitPointerLock();
      }
    });
  }

  private requestPointerLock(): void {
    document.body.requestPointerLock();
  }

  update(deltaTime: number): void {
    // Apply mouse look
    if (this.input.mouseDown) {
      this.yawObject.rotation.y -= this.input.mouseDelta.x * this.config.lookSpeed;
      this.pitchObject.rotation.x -= this.input.mouseDelta.y * this.config.lookSpeed;

      // Clamp pitch
      this.pitchObject.rotation.x = Math.max(
        -Math.PI / 2,
        Math.min(Math.PI / 2, this.pitchObject.rotation.x)
      );

      // Reset mouse delta
      this.input.mouseDelta.x = 0;
      this.input.mouseDelta.y = 0;
    }

    // Calculate movement direction
    const moveVector = new THREE.Vector3();
    const forward = new THREE.Vector3(0, 0, -1);
    const right = new THREE.Vector3(1, 0, 0);

    forward.applyQuaternion(this.yawObject.quaternion);
    right.applyQuaternion(this.yawObject.quaternion);

    // Remove Y component for horizontal movement
    forward.y = 0;
    right.y = 0;
    forward.normalize();
    right.normalize();

    // WASD movement
    if (this.input.keys.has('w')) {
      moveVector.add(forward);
    }
    if (this.input.keys.has('s')) {
      moveVector.sub(forward);
    }
    if (this.input.keys.has('a')) {
      moveVector.sub(right);
    }
    if (this.input.keys.has('d')) {
      moveVector.add(right);
    }

    // Vertical movement
    if (this.input.keys.has(' ')) { // Space
      moveVector.y = 1;
    }
    if (this.input.keys.has('control') || this.input.keys.has('meta')) { // Ctrl/Cmd
      moveVector.y = -1;
    }

    // Normalize diagonal movement
    if (moveVector.length() > 0) {
      moveVector.normalize();
    }

    // Apply acceleration
    const acceleration = moveVector.multiplyScalar(this.config.acceleration * deltaTime);
    this.velocity.add(acceleration);

    // Apply friction
    const friction = this.velocity.clone().multiplyScalar(-this.config.friction * deltaTime);
    this.velocity.add(friction);

    // Clamp velocity
    const horizontalVelocity = new THREE.Vector2(this.velocity.x, this.velocity.z);
    if (horizontalVelocity.length() > this.config.moveSpeed) {
      horizontalVelocity.normalize().multiplyScalar(this.config.moveSpeed);
      this.velocity.x = horizontalVelocity.x;
      this.velocity.z = horizontalVelocity.y;
    }

    // Clamp vertical velocity
    this.velocity.y = Math.max(
      -this.config.verticalSpeed,
      Math.min(this.config.verticalSpeed, this.velocity.y)
    );

    // Apply velocity to position
    const deltaPosition = this.velocity.clone().multiplyScalar(deltaTime);
    this.yawObject.position.add(deltaPosition);

    // Update camera world matrix
    this.camera.updateMatrixWorld();
  }

  getObject(): THREE.Object3D {
    return this.yawObject;
  }

  getPosition(): THREE.Vector3 {
    return this.yawObject.position.clone();
  }

  setPosition(position: THREE.Vector3): void {
    this.yawObject.position.copy(position);
  }

  getDirection(): THREE.Vector3 {
    const direction = new THREE.Vector3(0, 0, -1);
    direction.applyQuaternion(this.camera.quaternion);
    return direction;
  }

  dispose(): void {
    // Remove event listeners
    document.removeEventListener('keydown', this.initEventListeners);
    document.removeEventListener('keyup', this.initEventListeners);
    document.removeEventListener('mousedown', this.initEventListeners);
    document.removeEventListener('mouseup', this.initEventListeners);
    document.removeEventListener('mousemove', this.initEventListeners);
  }
} 