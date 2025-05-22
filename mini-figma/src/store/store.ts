import { create } from 'zustand';
import { type AppState, ToolType, type CanvasObject, type HandleType, type RectangleObject, type EllipseObject } from '../types/canvas';

// Simple ID generator
const generateId = () => Date.now().toString(36) + Math.random().toString(36).substring(2);

// Define the store state and actions separately for clarity
interface StoreActions {
  setActiveTool: (tool: ToolType) => void;
  addObject: (objectData: Omit<CanvasObject, 'id'>) => string;
  updateObject: (id: string, updates: Partial<CanvasObject>) => void;
  deleteObject: (id: string) => void;
  setSelectedObjectIds: (ids: string[]) => void;
  setCanvasView: (view: Partial<AppState['canvasView']>) => void;
  // Drawing state actions
  startDrawing: (x: number, y: number, objectId?: string) => void;
  updateDrawing: (x: number, y: number) => void; // Could be used for real-time updates
  endDrawing: () => void;
  // Dragging actions
  startDragging: (objectId: string, startX: number, startY: number, initialObjectX: number, initialObjectY: number) => void;
  endDragging: () => void;
  // Panning actions
  startPanning: (mouseX: number, mouseY: number, currentPanX: number, currentPanY: number) => void;
  updatePan: (mouseX: number, mouseY: number) => void; // New action to update pan based on mouse move
  endPanning: () => void;
  // Resizing actions
  startResizing: (objectId: string, handle: HandleType, mouseX: number, mouseY: number, objectSnapshot: Partial<RectangleObject | EllipseObject>) => void;
  endResizing: () => void;
}

// Initial state
const initialDrawingState: AppState['drawingState'] = {
  isDrawing: false,
  startPoint: null,
  currentObjectId: null,
  isDragging: false,
  dragObjectInitialX: undefined,
  dragObjectInitialY: undefined,
  isPanning: false,
  initialPanX: undefined,
  initialPanY: undefined,
  isResizing: false,
  activeHandle: undefined,
  resizeObjectSnapshot: undefined,
};

const initialState: AppState = {
  activeTool: ToolType.SELECT,
  objects: [],
  selectedObjectIds: [],
  canvasView: {
    panX: 0,
    panY: 0,
    zoom: 1,
  },
  drawingState: { ...initialDrawingState },
};

// Create the store with both state and actions
export const useStore = create<AppState & StoreActions>((set, get) => ({
  ...initialState,

  // Actions
  setActiveTool: (tool) => {
    get().endDrawing(); // End any active drawing when tool changes
    get().endDragging(); // Also end dragging when tool changes
    get().endPanning(); // End panning when tool changes
    get().endResizing(); // End resizing when tool changes
    set({ activeTool: tool });
  },

  addObject: (objectData) => {
    const newObject = { ...objectData, id: generateId() } as CanvasObject;
    set((state) => ({ objects: [...state.objects, newObject] }));
    return newObject.id; // Return new object's ID
  },

  updateObject: (id, updates) =>
    set((state) => ({
      objects: state.objects.map((obj) =>
        obj.id === id ? ({ ...obj, ...updates } as CanvasObject) : obj // Cast to CanvasObject
      ),
    })),

  deleteObject: (id) =>
    set((state) => ({
      objects: state.objects.filter((obj) => obj.id !== id),
      selectedObjectIds: state.selectedObjectIds.filter(selId => selId !== id),
    })),

  setSelectedObjectIds: (ids) => {
    get().endDragging(); // If selection changes, stop any active drag
    get().endResizing(); // End resizing on selection change
    // Do not end panning on selection change, as user might be panning to select
    set({ selectedObjectIds: ids });
  },

  setCanvasView: (view) =>
    set((state) => ({ canvasView: { ...state.canvasView, ...view } })),
  
  // Drawing state actions implementation
  startDrawing: (x, y, objectId) => {
    get().endDragging(); // Ensure not dragging when starting to draw
    get().endPanning();
    get().endResizing();
    set(state => ({
      drawingState: {
        ...initialDrawingState, // Reset to initial drawing state parts
        isDrawing: true,
        startPoint: { x, y },
        currentObjectId: objectId || null,
      }
    }));
  },

  updateDrawing: (x, y) => { // This action might be used by tools if needed for intermediate steps
    // For simple rectangle drawing, we directly update the object in mouseMove
    // but this action can be a placeholder for more complex drawing tools
    // For now, it doesn't modify state directly but shows where such logic could go.
  },

  endDrawing: () => {
    set(state => ({
      drawingState: {
        ...state.drawingState,
        isDrawing: false,
        // startPoint: null, // Usually cleared by the tool logic itself or on next startDrawing
        // currentObjectId: null, 
      }
    }));
  },

  startDragging: (objectId, startX, startY, initialObjectX, initialObjectY) => {
    get().endDrawing(); // Ensure not drawing when starting to drag
    get().endPanning();
    get().endResizing();
    set(state => ({
      drawingState: {
        ...initialDrawingState,
        isDragging: true,
        currentObjectId: objectId,
        startPoint: { x: startX, y: startY }, // Mouse start position
        dragObjectInitialX: initialObjectX,
        dragObjectInitialY: initialObjectY,
      }
    }));
  },

  endDragging: () => {
    set(state => ({
      drawingState: {
        ...state.drawingState,
        isDragging: false,
        // startPoint: null, // Cleared to prevent interference
        // dragObjectInitialX: undefined,
        // dragObjectInitialY: undefined,
      }
    }));
  },

  // Panning Actions
  startPanning: (mouseX, mouseY, currentPanX, currentPanY) => {
    get().endDrawing();
    get().endDragging();
    get().endResizing();
    set(state => ({
      drawingState: {
        ...initialDrawingState,
        isPanning: true,
        startPoint: { x: mouseX, y: mouseY }, // Mouse start for calculating delta
        initialPanX: currentPanX,
        initialPanY: currentPanY,
      }
    }));
  },

  updatePan: (mouseX, mouseY) => {
    const { drawingState: ds, canvasView: cv } = get();
    if (!ds.isPanning || !ds.startPoint || ds.initialPanX === undefined || ds.initialPanY === undefined) return;

    const deltaX = mouseX - ds.startPoint.x;
    const deltaY = mouseY - ds.startPoint.y;

    set({ 
        canvasView: { 
            ...cv, 
            panX: ds.initialPanX + deltaX, 
            panY: ds.initialPanY + deltaY 
        } 
    });
  },

  endPanning: () => {
    set(state => ({
      drawingState: {
        ...state.drawingState,
        isPanning: false,
        // startPoint: null, // Cleared to prevent interference
        // initialPanX: undefined,
        // initialPanY: undefined,
      }
    }));
  },

  // Resizing Actions
  startResizing: (objectId, handle, mouseX, mouseY, objectSnapshot) => {
    get().endDrawing(); get().endDragging(); get().endPanning();
    set(state => ({
      drawingState: {
        ...initialDrawingState,
        isResizing: true,
        currentObjectId: objectId,
        activeHandle: handle,
        startPoint: { x: mouseX, y: mouseY }, // Mouse start for calculating delta during resize
        resizeObjectSnapshot: objectSnapshot,
      }
    }));
  },

  endResizing: () => {
    set(state => ({
      drawingState: {
        ...state.drawingState,
        isResizing: false,
        activeHandle: undefined,
        // resizeObjectSnapshot: undefined, // Cleared when isResizing goes false or on next startResizing
      }
    }));
  },
}));

// Exporting the initial state might be useful for resetting or for specific scenarios
export { initialState as initialAppState }; 