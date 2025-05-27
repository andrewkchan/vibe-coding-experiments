# 3D Terrain Editor

A web-based 3D terrain editor built with TypeScript and Three.js, featuring real-time terrain manipulation with intuitive tools.

## Features

- **3D Terrain Visualization**: Wireframe mesh rendering of procedurally generated terrain
- **First-Person Camera**: Smooth FPS-style camera controls for navigating the scene
- **Terrain Editing Tools**:
  - Fill Tool: Raise terrain with natural Gaussian falloff
  - Dig Tool: Lower terrain with the same smooth falloff
- **Real-time Editing**: Click and drag to sculpt terrain in real-time
- **Procedural Generation**: Initial terrain generated using Simplex noise with multiple octaves
- **Customizable Brushes**: Adjustable brush size and strength
- **Clean UI**: Modern, semi-transparent panels with tool controls

## Getting Started

### Prerequisites

- Node.js (v14 or higher)
- npm or yarn

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd terrain-editor
```

2. Install dependencies:
```bash
npm install
```

3. Start the development server:
```bash
npm run dev
```

4. Open your browser and navigate to `http://localhost:3000`

### Building for Production

```bash
npm run build
```

The built files will be in the `dist` directory.

## Controls

### Camera Movement
- **WASD**: Move horizontally
- **Space**: Move up
- **Ctrl/Cmd**: Move down
- **Right Click + Drag**: Look around

### Terrain Editing
- **Left Click**: Apply current tool
- **Fill Tool**: Raises terrain at cursor position
- **Dig Tool**: Lowers terrain at cursor position

### UI Controls
- **Brush Size**: Adjust the radius of the editing brush
- **Brush Strength**: Control how quickly terrain changes
- **Reset Terrain**: Generate new random terrain

## Project Structure

```
terrain-editor/
├── src/
│   ├── main.ts                 # Application entry point
│   ├── types/                  # TypeScript type definitions
│   ├── terrain/                # Terrain generation and management
│   │   ├── TerrainGenerator.ts # Procedural terrain generation
│   │   ├── TerrainMesh.ts      # Three.js mesh management
│   │   └── HeightmapEditor.ts  # Tool interaction system
│   ├── camera/                 # Camera controls
│   │   └── FirstPersonCamera.ts
│   ├── tools/                  # Editing tools
│   │   ├── BaseTool.ts         # Base class for all tools
│   │   ├── FillTool.ts         # Terrain raising tool
│   │   └── DigTool.ts          # Terrain lowering tool
│   └── utils/                  # Utility functions
├── styles/                     # CSS styles
├── index.html                  # Main HTML file
├── package.json
├── tsconfig.json              # TypeScript configuration
└── vite.config.ts             # Vite build configuration
```

## Technical Details

### Terrain Generation
- Uses Simplex noise with configurable octaves for realistic terrain
- Edge falloff creates natural boundaries
- 128x128 vertex resolution by default

### Rendering
- Three.js for WebGL rendering
- Wireframe mesh visualization
- Real-time geometry updates

### Performance
- Efficient heightmap updates
- Optimized brush calculations
- 60 FPS target with smooth editing

## Future Enhancements

- Additional tools (Smooth, Flatten, Noise)
- Save/Load terrain functionality
- Texture-based rendering option
- Undo/Redo system
- Level-of-detail (LOD) for larger terrains

## License

This project is for educational purposes. 