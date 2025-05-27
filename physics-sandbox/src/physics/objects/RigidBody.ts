import * as THREE from 'three';
import * as CANNON from 'cannon-es';

export type RigidBodyShape = 'box' | 'cylinder' | 'cone';

export interface RigidBodyOptions {
  shape?: RigidBodyShape;
  size?: THREE.Vector3;
  color?: number;
  mass?: number;
  position?: THREE.Vector3;
  rotation?: THREE.Euler;
  velocity?: THREE.Vector3;
}

export class RigidBody {
  public mesh: THREE.Mesh;
  public body: CANNON.Body;
  private id: string;
  
  constructor(options: RigidBodyOptions = {}) {
    const {
      shape = 'box',
      size = new THREE.Vector3(2, 2, 2),
      color = 0x4ecdc4,
      mass = 1,
      position = new THREE.Vector3(0, 5, 0),
      rotation = new THREE.Euler(0, 0, 0),
      velocity = new THREE.Vector3(0, 0, 0)
    } = options;
    
    this.id = `rigid_${shape}_${Date.now()}_${Math.random()}`;
    
    // Create Three.js mesh based on shape
    let geometry: THREE.BufferGeometry;
    let physicsShape: CANNON.Shape;
    
    switch (shape) {
      case 'box':
        geometry = new THREE.BoxGeometry(size.x, size.y, size.z);
        physicsShape = new CANNON.Box(new CANNON.Vec3(size.x / 2, size.y / 2, size.z / 2));
        break;
        
      case 'cylinder':
        geometry = new THREE.CylinderGeometry(size.x / 2, size.x / 2, size.y, 16);
        // Cannon-es doesn't have a cylinder shape, so we approximate with a convex polyhedron
        physicsShape = this.createCylinderShape(size.x / 2, size.y);
        break;
        
      case 'cone':
        geometry = new THREE.ConeGeometry(size.x / 2, size.y, 16);
        // Similar approximation for cone
        physicsShape = this.createConeShape(size.x / 2, size.y);
        break;
        
      default:
        geometry = new THREE.BoxGeometry(size.x, size.y, size.z);
        physicsShape = new CANNON.Box(new CANNON.Vec3(size.x / 2, size.y / 2, size.z / 2));
    }
    
    const material = new THREE.MeshPhongMaterial({
      color,
      shininess: 80,
      specular: 0xffffff,
      emissive: color,
      emissiveIntensity: 0.05
    });
    
    this.mesh = new THREE.Mesh(geometry, material);
    this.mesh.castShadow = true;
    this.mesh.receiveShadow = true;
    this.mesh.position.copy(position);
    this.mesh.rotation.copy(rotation);
    
    // Create physics body
    const bodyMaterial = new CANNON.Material('rigidBody');
    
    this.body = new CANNON.Body({
      mass,
      shape: physicsShape,
      material: bodyMaterial,
      position: new CANNON.Vec3(position.x, position.y, position.z),
      velocity: new CANNON.Vec3(velocity.x, velocity.y, velocity.z)
    });
    
    // Apply rotation to physics body
    const quaternion = new THREE.Quaternion();
    quaternion.setFromEuler(rotation);
    this.body.quaternion.set(quaternion.x, quaternion.y, quaternion.z, quaternion.w);
    
    // Set material properties
    bodyMaterial.friction = 0.4;
    bodyMaterial.restitution = 0.3;
  }
  
  private createCylinderShape(radius: number, height: number): CANNON.Shape {
    // Create a cylinder using ConvexPolyhedron
    const vertices: CANNON.Vec3[] = [];
    const faces: number[][] = [];
    const segments = 16;
    
    // Create vertices
    for (let i = 0; i < segments; i++) {
      const angle = (i / segments) * Math.PI * 2;
      const x = Math.cos(angle) * radius;
      const z = Math.sin(angle) * radius;
      
      vertices.push(new CANNON.Vec3(x, height / 2, z));
      vertices.push(new CANNON.Vec3(x, -height / 2, z));
    }
    
    // Create side faces
    for (let i = 0; i < segments; i++) {
      const next = (i + 1) % segments;
      faces.push([i * 2, next * 2, next * 2 + 1, i * 2 + 1]);
    }
    
    // Create top and bottom faces
    const topFace: number[] = [];
    const bottomFace: number[] = [];
    for (let i = 0; i < segments; i++) {
      topFace.push(i * 2);
      bottomFace.push(i * 2 + 1);
    }
    faces.push(topFace);
    faces.push(bottomFace.reverse());
    
    return new CANNON.ConvexPolyhedron({ vertices, faces });
  }
  
  private createConeShape(radius: number, height: number): CANNON.Shape {
    // Create a cone using ConvexPolyhedron
    const vertices: CANNON.Vec3[] = [];
    const faces: number[][] = [];
    const segments = 16;
    
    // Add apex vertex
    vertices.push(new CANNON.Vec3(0, height / 2, 0));
    
    // Add base vertices
    for (let i = 0; i < segments; i++) {
      const angle = (i / segments) * Math.PI * 2;
      const x = Math.cos(angle) * radius;
      const z = Math.sin(angle) * radius;
      vertices.push(new CANNON.Vec3(x, -height / 2, z));
    }
    
    // Create side faces
    for (let i = 0; i < segments; i++) {
      const next = (i + 1) % segments;
      faces.push([0, i + 1, next + 1]);
    }
    
    // Create base face
    const baseFace: number[] = [];
    for (let i = segments; i > 0; i--) {
      baseFace.push(i);
    }
    faces.push(baseFace);
    
    return new CANNON.ConvexPolyhedron({ vertices, faces });
  }
  
  public getId(): string {
    return this.id;
  }
  
  public setColor(color: number): void {
    (this.mesh.material as THREE.MeshPhongMaterial).color.setHex(color);
    (this.mesh.material as THREE.MeshPhongMaterial).emissive.setHex(color);
  }
  
  public applyImpulse(force: THREE.Vector3, point?: THREE.Vector3): void {
    const impulse = new CANNON.Vec3(force.x, force.y, force.z);
    if (point) {
      const worldPoint = new CANNON.Vec3(point.x, point.y, point.z);
      this.body.applyImpulse(impulse, worldPoint);
    } else {
      this.body.applyImpulse(impulse);
    }
  }
  
  public applyTorque(torque: THREE.Vector3): void {
    this.body.torque.set(torque.x, torque.y, torque.z);
  }
  
  public reset(position: THREE.Vector3, rotation?: THREE.Euler): void {
    this.body.position.set(position.x, position.y, position.z);
    this.body.velocity.set(0, 0, 0);
    this.body.angularVelocity.set(0, 0, 0);
    
    if (rotation) {
      const quaternion = new THREE.Quaternion();
      quaternion.setFromEuler(rotation);
      this.body.quaternion.set(quaternion.x, quaternion.y, quaternion.z, quaternion.w);
    }
  }
  
  // Helper to get random bright colors for cartoon look
  public static getRandomColor(): number {
    const colors = [
      0x4ecdc4, // Turquoise
      0xf39c12, // Orange
      0x9b59b6, // Purple
      0xe74c3c, // Red
      0x3498db, // Blue
      0x2ecc71, // Green
      0x1abc9c, // Teal
      0xf1c40f, // Yellow
      0xe67e22, // Carrot
      0x34495e  // Dark Blue
    ];
    return colors[Math.floor(Math.random() * colors.length)];
  }
} 