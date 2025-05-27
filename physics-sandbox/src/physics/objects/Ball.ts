import * as THREE from 'three';
import * as CANNON from 'cannon-es';

export interface BallOptions {
  radius?: number;
  color?: number;
  mass?: number;
  restitution?: number;
  position?: THREE.Vector3;
  velocity?: THREE.Vector3;
}

export class Ball {
  public mesh: THREE.Mesh;
  public body: CANNON.Body;
  private id: string;
  
  constructor(options: BallOptions = {}) {
    const {
      radius = 1,
      color = 0xff6b6b,
      mass = 1,
      restitution = 0.7,
      position = new THREE.Vector3(0, 5, 0),
      velocity = new THREE.Vector3(0, 0, 0)
    } = options;
    
    this.id = `ball_${Date.now()}_${Math.random()}`;
    
    // Create Three.js mesh
    const geometry = new THREE.SphereGeometry(radius, 32, 16);
    const material = new THREE.MeshPhongMaterial({
      color,
      shininess: 100,
      specular: 0xffffff,
      emissive: color,
      emissiveIntensity: 0.1
    });
    
    this.mesh = new THREE.Mesh(geometry, material);
    this.mesh.castShadow = true;
    this.mesh.receiveShadow = true;
    this.mesh.position.copy(position);
    
    // Create physics body
    const shape = new CANNON.Sphere(radius);
    const ballMaterial = new CANNON.Material('ball');
    
    this.body = new CANNON.Body({
      mass,
      shape,
      material: ballMaterial,
      position: new CANNON.Vec3(position.x, position.y, position.z),
      velocity: new CANNON.Vec3(velocity.x, velocity.y, velocity.z)
    });
    
    // Set restitution (bounciness)
    ballMaterial.restitution = restitution;
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
  
  public reset(position: THREE.Vector3): void {
    this.body.position.set(position.x, position.y, position.z);
    this.body.velocity.set(0, 0, 0);
    this.body.angularVelocity.set(0, 0, 0);
  }
  
  // Helper to get random bright colors for cartoon look
  public static getRandomColor(): number {
    const colors = [
      0xff6b6b, // Red
      0x4ecdc4, // Turquoise
      0x45b7d1, // Blue
      0xf9ca24, // Yellow
      0x6c5ce7, // Purple
      0xa29bfe, // Light Purple
      0xfd79a8, // Pink
      0xfdcb6e, // Orange
      0x00b894, // Green
      0xe17055  // Coral
    ];
    return colors[Math.floor(Math.random() * colors.length)];
  }
} 