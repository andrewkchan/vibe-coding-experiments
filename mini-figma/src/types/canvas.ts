// Enum for different tool types
export enum ToolType {
  SELECT = 'SELECT',
  RECTANGLE = 'RECTANGLE',
  ELLIPSE = 'ELLIPSE',
  PEN = 'PEN',
  TEXT = 'TEXT',
}

// Base interface for all canvas objects
export interface CanvasObjectBase {
  id: string;
  x: number;
  y: number;
  opacity: number; // 0.0 to 1.0
  // TODO: Add rotation, fill, stroke properties later
}

// Specific object types
export interface RectangleObject extends CanvasObjectBase {
  type: 'rectangle';
  width: number;
  height: number;
  fillColor: string;
  strokeColor?: string;
  strokeWidth?: number;
}

export interface EllipseObject extends CanvasObjectBase {
  type: 'ellipse';
  radiusX: number;
  radiusY: number;
  fillColor: string;
  strokeColor?: string;
  strokeWidth?: number;
}

export interface PathObject extends CanvasObjectBase {
  type: 'path';
  points: { x: number; y: number }[]; // Array of points for the path
  strokeColor: string;
  strokeWidth: number;
  // Paths are typically not filled in this context, but could be extended
}

export interface TextObject extends CanvasObjectBase {
  type: 'text';
  content: string;
  fontSize: number;
  fontFamily: string;
  fillColor: string;
  // TODO: Add more text properties like textAlign, fontWeight etc. later
}

// Union type for any canvas object
export type CanvasObject = RectangleObject | EllipseObject | PathObject | TextObject;

// Type for the application state (will be expanded)
export interface AppState {
  activeTool: ToolType;
  objects: CanvasObject[];
  selectedObjectIds: string[];
  canvasView: {
    panX: number;
    panY: number;
    zoom: number;
  };
  drawingState: {
    isDrawing: boolean;
    startPoint: { x: number; y: number } | null;
    currentObjectId: string | null; // ID of the object being actively drawn/resized
    // We might add tool-specific drawing state here later if needed
  };
} 