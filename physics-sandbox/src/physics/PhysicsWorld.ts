import * as CANNON from 'cannon-es';
import * as THREE from 'three';

export class PhysicsWorld {
  private world: CANNON.World;
  private bodies: Map<string, CANNON.Body> = new Map();
  private meshes: Map<string, THREE.Object3D> = new Map();
  private debugMode: boolean = false;
  
  constructor() {
    // Initialize physics world
    this.world = new CANNON.World({
      gravity: new CANNON.Vec3(0, -9.82, 0)
    });
    
    // Set physics properties for better simulation
    this.world.broadphase = new CANNON.NaiveBroadphase();
    
    // Better solver settings for stable constraints
    if ('tolerance' in this.world.solver) {
      (this.world.solver as any).tolerance = 0.001;
    }
    if ('iterations' in this.world.solver) {
      (this.world.solver as any).iterations = 20;
    }
    
    // Softer contact materials for better stability
    this.world.defaultContactMaterial.contactEquationStiffness = 1e6;
    this.world.defaultContactMaterial.contactEquationRelaxation = 3;
    this.world.defaultContactMaterial.frictionEquationStiffness = 1e6;
    this.world.defaultContactMaterial.frictionEquationRelaxation = 3;
    
    // Create ground physics body
    this.createGroundBody();
  }
  
  private createGroundBody(): void {
    const groundShape = new CANNON.Box(new CANNON.Vec3(50, 0.1, 50));
    const groundBody = new CANNON.Body({
      mass: 0, // Static body
      shape: groundShape,
      position: new CANNON.Vec3(0, -0.1, 0)
    });
    
    this.world.addBody(groundBody);
    this.bodies.set('ground', groundBody);
  }
  
  public addBody(id: string, body: CANNON.Body, mesh: THREE.Object3D): void {
    this.world.addBody(body);
    this.bodies.set(id, body);
    this.meshes.set(id, mesh);
  }
  
  public removeBody(id: string): void {
    const body = this.bodies.get(id);
    const mesh = this.meshes.get(id);
    
    if (body) {
      this.world.removeBody(body);
      this.bodies.delete(id);
    }
    
    if (mesh) {
      this.meshes.delete(id);
    }
  }
  
  public update(deltaTime: number): void {
    // Step the physics world
    this.world.step(deltaTime);
    
    // Update mesh positions and rotations based on physics bodies
    this.bodies.forEach((body, id) => {
      const mesh = this.meshes.get(id);
      if (mesh && id !== 'ground') {
        mesh.position.copy(body.position as any);
        mesh.quaternion.copy(body.quaternion as any);
      }
    });
  }
  
  public setGravity(x: number, y: number, z: number): void {
    this.world.gravity.set(x, y, z);
  }
  
  public getWorld(): CANNON.World {
    return this.world;
  }
  
  public getBodies(): Map<string, CANNON.Body> {
    return this.bodies;
  }
  
  public getBody(id: string): CANNON.Body | undefined {
    return this.bodies.get(id);
  }
  
  public setDebugMode(enabled: boolean): void {
    this.debugMode = enabled;
  }
  
  public reset(): void {
    // Remove all bodies except ground
    const bodiesToRemove: string[] = [];
    this.bodies.forEach((body, id) => {
      if (id !== 'ground') {
        bodiesToRemove.push(id);
      }
    });
    
    bodiesToRemove.forEach(id => this.removeBody(id));
  }
  
  // Helper method to create contact materials
  public createContactMaterial(
    material1: CANNON.Material,
    material2: CANNON.Material,
    options: {
      friction?: number;
      restitution?: number;
      contactEquationStiffness?: number;
      contactEquationRelaxation?: number;
    }
  ): CANNON.ContactMaterial {
    const contactMaterial = new CANNON.ContactMaterial(material1, material2, {
      friction: options.friction ?? 0.4,
      restitution: options.restitution ?? 0.3,
      contactEquationStiffness: options.contactEquationStiffness ?? 1e7,
      contactEquationRelaxation: options.contactEquationRelaxation ?? 3
    });
    
    this.world.addContactMaterial(contactMaterial);
    return contactMaterial;
  }
} 