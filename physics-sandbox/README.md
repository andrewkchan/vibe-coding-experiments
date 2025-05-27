# Physics Sandbox

A web-based 3D physics sandbox with cartoon-style graphics where you can interact with various physics objects including bouncing balls, rigid bodies, and cloth simulation.

## Features

- **3D Environment**: Beautiful cartoon-style world with sky blue background and checkered green ground
- **Physics Objects**: Balls, boxes, cylinders, cones, and cloth with realistic physics
- **Two Control Modes**: Separate camera and interaction modes for better control
- **Real-time Physics**: Adjustable gravity, time scale, and wind effects
- **Mouse & Keyboard Controls**: Intuitive controls for both viewing and interacting

## Getting Started

```bash
npm install
npm run dev
```

Open your browser and navigate to `http://localhost:5173` (or the port shown in terminal).

## Control Modes

The sandbox features two distinct control modes to prevent accidental actions:

### 🎥 Camera Mode (Default)
- **Purpose**: Navigate and view the 3D scene
- **Activation**: Press `C` or click the Camera button
- **Controls**:
  - Left Click + Drag: Rotate camera around the scene
  - Right Click + Drag: Pan the camera
  - Scroll Wheel: Zoom in/out
- **Visual Indicator**: Blue border around viewport

### ✋ Interact Mode
- **Purpose**: Spawn, move, and delete physics objects
- **Activation**: Press `I` or click the Interact button
- **Controls**:
  - Left Click: Spawn selected object type
  - Shift + Left Click + Drag: Move existing objects
  - Right Click: Delete objects
- **Visual Indicator**: Red border around viewport

### Quick Mode Switching
- Press `ESC` to quickly return to Camera Mode
- Mode indicator at top center shows current mode
- Object toolbar only visible in Interact Mode

## Physics Controls

### Control Panel (Top Left)
- **Gravity**: Adjust gravity strength (-20 to +20)
- **Time Scale**: Speed up or slow down physics (0 to 2x)
- **Wind Force**: Apply wind to cloth objects (-10 to +10)
- **Reset Scene**: Reset physics without removing objects
- **Clear All**: Remove all spawned objects

### Object Types (Bottom Toolbar - Interact Mode Only)
- ⚪ **Ball**: Bouncing spheres with random colors and sizes
- ◼️ **Box**: Rectangular rigid bodies
- ⬭ **Cylinder**: Cylindrical objects
- △ **Cone**: Cone-shaped objects
- 🏴 **Cloth**: Flexible cloth simulation with wind interaction

## Tips

1. Start in Camera Mode to find a good viewing angle
2. Switch to Interact Mode when you want to add objects
3. Use Shift+Drag in Interact Mode to reposition objects
4. Adjust wind force to see cloth react dynamically
5. Experiment with different gravity settings for interesting effects

## Technical Details

- Built with Three.js for 3D graphics
- Cannon-es for physics simulation
- TypeScript for type safety
- Vite for fast development

## Browser Requirements

- Modern browser with WebGL support
- Chrome, Firefox, Safari, or Edge (latest versions)
- Hardware acceleration enabled for best performance 