import { create } from 'zustand';
import { type AppState, ToolType, type CanvasObject } from '../types/canvas';

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
}

// Initial state
const initialState: AppState = {
  activeTool: ToolType.SELECT,
  objects: [],
  selectedObjectIds: [],
  canvasView: {
    panX: 0,
    panY: 0,
    zoom: 1,
  },
  drawingState: {
    isDrawing: false,
    startPoint: null,
    currentObjectId: null,
  },
};

// Create the store with both state and actions
export const useStore = create<AppState & StoreActions>((set, get) => ({
  ...initialState,

  // Actions
  setActiveTool: (tool) => {
    get().endDrawing(); // End any active drawing when tool changes
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

  setSelectedObjectIds: (ids) => set({ selectedObjectIds: ids }),

  setCanvasView: (view) =>
    set((state) => ({ canvasView: { ...state.canvasView, ...view } })),
  
  // Drawing state actions implementation
  startDrawing: (x, y, objectId) => {
    set(state => ({
      drawingState: {
        ...state.drawingState,
        isDrawing: true,
        startPoint: { x, y },
        currentObjectId: objectId || null, // If an objectId is passed (e.g. for resizing), store it
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
        startPoint: null,
        currentObjectId: null,
      }
    }));
  },
}));

// Exporting the initial state might be useful for resetting or for specific scenarios
export { initialState as initialAppState }; 