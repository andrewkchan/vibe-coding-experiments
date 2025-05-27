import * as THREE from 'three';
import * as CANNON from 'cannon-es';

export interface ClothOptions {
  width?: number;
  height?: number;
  segments?: number;
  color?: number;
  mass?: number;
  position?: THREE.Vector3;
  pinned?: boolean[][];
}

export class Cloth {
  public mesh: THREE.Mesh;
  public particles: CANNON.Body[] = [];
  public constraints: CANNON.DistanceConstraint[] = [];
  private id: string;
  private geometry: THREE.BufferGeometry;
  private positions: Float32Array;
  private width: number;
  private height: number;
  private segments: number;
  
  constructor(options: ClothOptions = {}) {
    const {
      width = 10,
      height = 10,
      segments = 15,
      color = 0xe74c3c,
      mass = 0.1,
      position = new THREE.Vector3(0, 10, 0),
      pinned = []
    } = options;
    
    this.id = `cloth_${Date.now()}_${Math.random()}`;
    this.width = width;
    this.height = height;
    this.segments = segments;
    
    // Create geometry
    this.geometry = new THREE.PlaneGeometry(width, height, segments, segments);
    this.positions = this.geometry.attributes.position.array as Float32Array;
    
    // Create material with double side rendering for cloth
    const material = new THREE.MeshPhongMaterial({
      color,
      side: THREE.DoubleSide,
      shininess: 40,
      specular: 0x222222,
      emissive: color,
      emissiveIntensity: 0.02
    });
    
    this.mesh = new THREE.Mesh(this.geometry, material);
    this.mesh.castShadow = true;
    this.mesh.receiveShadow = true;
    this.mesh.position.copy(position);
    
    // Create physics particles for each vertex
    this.createParticles(mass, position, pinned);
    
    // Create constraints between particles
    this.createConstraints();
    
    // Make geometry dynamic
    (this.geometry.attributes.position as THREE.BufferAttribute).setUsage(THREE.DynamicDrawUsage);
  }
  
  private createParticles(mass: number, offset: THREE.Vector3, pinned: boolean[][]): void {
    const particlesPerRow = this.segments + 1;
    const particleMass = mass / (particlesPerRow * particlesPerRow);
    
    for (let y = 0; y <= this.segments; y++) {
      for (let x = 0; x <= this.segments; x++) {
        const index = y * particlesPerRow + x;
        const vertexIndex = index * 3;
        
        const px = this.positions[vertexIndex] + offset.x;
        const py = this.positions[vertexIndex + 1] + offset.y;
        const pz = this.positions[vertexIndex + 2] + offset.z;
        
        // Check if this particle should be pinned
        const isPinned = pinned[y] && pinned[y][x];
        
        const particle = new CANNON.Body({
          mass: isPinned ? 0 : particleMass, // 0 mass makes it static
          position: new CANNON.Vec3(px, py, pz),
          shape: new CANNON.Particle(),
          // Reduce damping for more fluid motion
          linearDamping: 0.01,
          angularDamping: 0.01
        });
        
        this.particles.push(particle);
      }
    }
    
    // Pin top corners by default if no pinning specified
    if (pinned.length === 0) {
      this.particles[0].mass = 0; // Top left
      this.particles[this.segments].mass = 0; // Top right
    }
  }
  
  private createConstraints(): void {
    const particlesPerRow = this.segments + 1;
    const restDistance = this.width / this.segments;
    
    // Structural constraints (horizontal and vertical)
    for (let y = 0; y <= this.segments; y++) {
      for (let x = 0; x <= this.segments; x++) {
        const index = y * particlesPerRow + x;
        
        // Horizontal constraint
        if (x < this.segments) {
          const constraint = new CANNON.DistanceConstraint(
            this.particles[index],
            this.particles[index + 1],
            restDistance
          );
          this.constraints.push(constraint);
        }
        
        // Vertical constraint
        if (y < this.segments) {
          const constraint = new CANNON.DistanceConstraint(
            this.particles[index],
            this.particles[index + particlesPerRow],
            restDistance
          );
          this.constraints.push(constraint);
        }
        
        // Diagonal constraints for stability
        if (x < this.segments && y < this.segments) {
          // Diagonal 1
          const constraint1 = new CANNON.DistanceConstraint(
            this.particles[index],
            this.particles[index + particlesPerRow + 1],
            restDistance * Math.sqrt(2)
          );
          this.constraints.push(constraint1);
          
          // Diagonal 2
          const constraint2 = new CANNON.DistanceConstraint(
            this.particles[index + 1],
            this.particles[index + particlesPerRow],
            restDistance * Math.sqrt(2)
          );
          this.constraints.push(constraint2);
        }
      }
    }
  }
  
  public updateGeometry(): void {
    const particlesPerRow = this.segments + 1;
    
    for (let y = 0; y <= this.segments; y++) {
      for (let x = 0; x <= this.segments; x++) {
        const index = y * particlesPerRow + x;
        const vertexIndex = index * 3;
        const particle = this.particles[index];
        
        // Update vertex positions from particle positions
        this.positions[vertexIndex] = particle.position.x - this.mesh.position.x;
        this.positions[vertexIndex + 1] = particle.position.y - this.mesh.position.y;
        this.positions[vertexIndex + 2] = particle.position.z - this.mesh.position.z;
      }
    }
    
    // Notify Three.js that positions have changed
    this.geometry.attributes.position.needsUpdate = true;
    this.geometry.computeVertexNormals();
  }
  
  public applyWind(force: THREE.Vector3): void {
    const windForce = new CANNON.Vec3(force.x, force.y, force.z);
    
    this.particles.forEach(particle => {
      if (particle.mass > 0) {
        particle.applyForce(windForce);
      }
    });
  }
  
  public pinParticle(x: number, y: number): void {
    const index = y * (this.segments + 1) + x;
    if (index >= 0 && index < this.particles.length) {
      this.particles[index].mass = 0;
    }
  }
  
  public unpinParticle(x: number, y: number): void {
    const particlesPerRow = this.segments + 1;
    const particleMass = 0.1 / (particlesPerRow * particlesPerRow);
    const index = y * (this.segments + 1) + x;
    
    if (index >= 0 && index < this.particles.length) {
      this.particles[index].mass = particleMass;
    }
  }
  
  public getId(): string {
    return this.id;
  }
  
  public getParticles(): CANNON.Body[] {
    return this.particles;
  }
  
  public getConstraints(): CANNON.DistanceConstraint[] {
    return this.constraints;
  }
  
  public reset(position: THREE.Vector3): void {
    const particlesPerRow = this.segments + 1;
    
    for (let y = 0; y <= this.segments; y++) {
      for (let x = 0; x <= this.segments; x++) {
        const index = y * particlesPerRow + x;
        const vertexIndex = index * 3;
        const particle = this.particles[index];
        
        // Reset to original position
        particle.position.x = this.positions[vertexIndex] + position.x;
        particle.position.y = this.positions[vertexIndex + 1] + position.y;
        particle.position.z = this.positions[vertexIndex + 2] + position.z;
        particle.velocity.set(0, 0, 0);
      }
    }
  }
} 