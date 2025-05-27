# Physics Sandbox - Development Plan

## Project Overview
A web-based 3D physics sandbox that allows users to interact with various physics objects (bouncing balls, rigid bodies, and cloth) using their mouse in real-time.

## Technology Stack
- **Three.js**: For 3D graphics rendering
- **Cannon.js** or **Cannon-es**: For physics simulation
- **Vite**: For fast development and building
- **TypeScript**: For type safety and better development experience
- **HTML5 Canvas**: For rendering

## Core Features

### 1. 3D Environment
- Perspective camera with orbit controls
- Ground plane with grid
- Ambient and directional lighting
- Skybox or gradient background
- Camera controls (zoom, pan, rotate)

### 2. Physics Objects

#### Bouncing Balls
- Various sizes and colors
- Realistic bounce behavior
- Mass and restitution properties
- Spawn at mouse click position

#### Rigid Bodies
- Boxes, cylinders, and other shapes
- Realistic collision detection
- Gravity and friction
- Ability to throw/launch objects

#### Cloth Simulation
- Grid-based cloth mesh
- Pin/unpin vertices
- Wind effect
- Tear capability (stretch limit)

### 3. Mouse Interactions
- Click to spawn objects
- Drag to move objects
- Right-click for context menu
- Hover effects
- Object selection and manipulation

### 4. UI Controls
- Object spawning toolbar
- Physics properties panel
- Scene controls (gravity, time scale)
- Reset scene button
- Performance stats display

### 5. Additional Features
- Object deletion
- Pause/resume simulation
- Save/load scenes
- Different materials (metal, rubber, wood)
- Constraints (hinges, springs)
- Force application tools

## Project Structure
```
physics-sandbox/
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
├── src/
│   ├── main.ts
│   ├── scene/
│   │   ├── SceneManager.ts
│   │   ├── Camera.ts
│   │   └── Lighting.ts
│   ├── physics/
│   │   ├── PhysicsWorld.ts
│   │   ├── objects/
│   │   │   ├── Ball.ts
│   │   │   ├── RigidBody.ts
│   │   │   └── Cloth.ts
│   │   └── constraints/
│   ├── controls/
│   │   ├── MouseControls.ts
│   │   └── UIControls.ts
│   ├── utils/
│   │   └── helpers.ts
│   └── styles/
│       └── main.css
└── public/
    └── textures/
```

## Development Phases

### Phase 1: Basic Setup
1. Initialize project with Vite and TypeScript
2. Set up Three.js scene with camera and lighting
3. Implement basic orbit controls
4. Create ground plane

### Phase 2: Physics Integration
1. Integrate Cannon-es physics engine
2. Sync physics world with Three.js scene
3. Implement basic gravity and collision

### Phase 3: Object Implementation
1. Create bouncing ball class
2. Implement rigid body shapes
3. Add basic cloth simulation

### Phase 4: Interactions
1. Mouse picking/raycasting
2. Object spawning on click
3. Drag and drop functionality
4. Object selection system

### Phase 5: UI and Controls
1. Create HTML UI overlay
2. Implement control panels
3. Add material selection
4. Performance monitoring

### Phase 6: Polish and Features
1. Add visual effects (shadows, particles)
2. Implement save/load functionality
3. Add sound effects (optional)
4. Performance optimization

## Questions for Clarification

1. **Visual Style**: Do you prefer a realistic look or more stylized/colorful graphics?
2. **Performance Target**: Should this work on mobile devices or is desktop-only acceptable?
3. **Cloth Complexity**: Simple flag-like cloth or more complex clothing/fabric simulations?
4. **Physics Accuracy**: Game-like physics or more realistic simulation?
5. **Additional Objects**: Any specific objects beyond balls, boxes, and cloth? (e.g., chains, ropes, fluids)

## Next Steps
Once you confirm the plan or provide clarifications, I'll begin implementation starting with the basic project setup and gradually building up the features. 