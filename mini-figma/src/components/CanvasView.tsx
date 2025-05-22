import React, { useRef, useEffect, useCallback, useState } from 'react';
import { useStore } from '../store/store';
import { ToolType, type CanvasObject, type RectangleObject, type EllipseObject, type PathObject, type TextObject, type HandleType } from '../types/canvas';

const DEFAULT_RECT_WIDTH = 100;
const DEFAULT_RECT_HEIGHT = 100;
const DEFAULT_ELLIPSE_RADIUS_X = 60; // This will be radius, so default width is 2*this
const DEFAULT_ELLIPSE_RADIUS_Y = 40; // This will be radius, so default height is 2*this
const CLICK_DRAG_THRESHOLD = 5; // Pixels to differentiate click from drag
const DEFAULT_PATH_STROKE_COLOR = '#333333';
const DEFAULT_PATH_STROKE_WIDTH = 2;
const PATH_CLOSING_THRESHOLD = 10; // Pixels for detecting click near start point

// Selection visual constants
const SELECTION_COLOR = '#007bff';
const SELECTION_LINE_WIDTH = 1;
const SELECTION_HANDLE_SIZE = 8;
const PATH_POINT_SELECTION_RADIUS = 5;
const PATH_SEGMENT_SELECT_TOLERANCE_BUFFER = 3; // Extra pixels for easier segment selection

// Text tool defaults
const DEFAULT_TEXT_CONTENT = "Text";
const DEFAULT_FONT_SIZE = 24; // in pixels
const DEFAULT_FONT_FAMILY = 'Arial';
const DEFAULT_TEXT_COLOR = '#333333';
const ZOOM_SENSITIVITY = 0.001;

const HANDLE_SIZE = 8; // Visual size of handles on screen
const HANDLE_HIT_AREA_PADDING = 4; // Extra padding for easier handle clicking

const CanvasView: React.FC = () => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  // For Pen tool preview line
  const [previewLineEndPoint, setPreviewLineEndPoint] = useState<{x: number, y: number} | null>(null);
  const [isSpacebarDown, setIsSpacebarDown] = useState(false);
  const [hoveredHandle, setHoveredHandle] = useState<HandleType | null>(null);

  // Get state and actions from the store
  const {
    activeTool,
    objects,
    drawingState,
    canvasView,
    selectedObjectIds, // Added for selection
    addObject,
    updateObject,
    startDrawing,
    endDrawing,
    setSelectedObjectIds, // Added for selection
    startDragging, // Added
    endDragging,   // Added
    startPanning,
    updatePan,
    endPanning,
    setCanvasView, // For zoom
    startResizing, // Added
    endResizing,   // Added
  } = useStore((state) => state);

  // Convert screen coordinates (e.g., mouse event) to canvas/world coordinates
  const screenToWorld = (screenX: number, screenY: number): { x: number; y: number } => {
    const canvas = canvasRef.current;
    if (!canvas) return { x: 0, y: 0 };
    const rect = canvas.getBoundingClientRect();
    // 1. Adjust for canvas position on screen
    let worldX = screenX - rect.left;
    let worldY = screenY - rect.top;
    // 2. Adjust for panning
    worldX -= canvasView.panX;
    worldY -= canvasView.panY;
    // 3. Adjust for zooming
    worldX /= canvasView.zoom;
    worldY /= canvasView.zoom;
    return { x: worldX, y: worldY };
  };

  const getMouseCanvasCoordinates = (event: React.MouseEvent | MouseEvent): { x: number; y: number } => {
    return screenToWorld(event.clientX, event.clientY);
  };

  // Hit testing functions
  const isPointInRect = (px: number, py: number, rect: RectangleObject): boolean => {
    return px >= rect.x && px <= rect.x + rect.width && py >= rect.y && py <= rect.y + rect.height;
  };

  const isPointInEllipse = (px: number, py: number, ellipse: EllipseObject): boolean => {
    const dx = px - ellipse.x;
    const dy = py - ellipse.y;
    // Avoid division by zero if radius is 0 (ellipse not fully formed yet)
    if (ellipse.radiusX === 0 || ellipse.radiusY === 0) return false;
    return (dx * dx) / (ellipse.radiusX * ellipse.radiusX) + (dy * dy) / (ellipse.radiusY * ellipse.radiusY) <= 1;
  };

  // Helper function: distance squared between two points
  const distSq = (p1x: number, p1y: number, p2x: number, p2y: number): number => {
    return Math.pow(p1x - p2x, 2) + Math.pow(p1y - p2y, 2);
  };

  // Helper function: distance from a point to a line segment
  const isPointNearLineSegment = (
    px: number, py: number, 
    p1x: number, p1y: number, 
    p2x: number, p2y: number, 
    tolerance: number
  ): boolean => {
    const l2 = distSq(p1x, p1y, p2x, p2y);
    if (l2 === 0) return Math.sqrt(distSq(px, py, p1x, p1y)) <= tolerance; // Segment is a point

    let t = ((px - p1x) * (p2x - p1x) + (py - p1y) * (p2y - p1y)) / l2;
    t = Math.max(0, Math.min(1, t)); // Clamp t to the segment

    const closestX = p1x + t * (p2x - p1x);
    const closestY = p1y + t * (p2y - p1y);

    return Math.sqrt(distSq(px, py, closestX, closestY)) <= tolerance;
  };

  const isPointOnPath = (px: number, py: number, path: PathObject): boolean => {
    if (path.points.length < 2) { // Need at least two points to form a segment
      // If only one point, check proximity to that point (like a node)
      return path.points.length === 1 && 
             Math.sqrt(distSq(px, py, path.points[0].x, path.points[0].y)) <= (path.strokeWidth / 2 + PATH_SEGMENT_SELECT_TOLERANCE_BUFFER);
    }
    const tolerance = (path.strokeWidth / 2) + PATH_SEGMENT_SELECT_TOLERANCE_BUFFER;
    for (let i = 0; i < path.points.length - 1; i++) {
      const p1 = path.points[i];
      const p2 = path.points[i+1];
      if (isPointNearLineSegment(px, py, p1.x, p1.y, p2.x, p2.y, tolerance)) {
        return true;
      }
    }
    // Also check if closing segment is clicked, if path appears closed (last point equals first)
    if (path.points.length > 2) {
        const firstPoint = path.points[0];
        const lastPoint = path.points[path.points.length -1];
        if (firstPoint.x === lastPoint.x && firstPoint.y === lastPoint.y) {
            // Path is visually closed by points, no extra segment to check
        } else {
            // Check the segment from last physical point back to first if it's a non-closed path visually (for selection)
            // This is more for open paths. For paths explicitly closed by a point on top of first, the loop above handles it.
        }
    }
    return false;
  };

  // Hit testing for text (basic bounding box for now)
  const isPointInText = (px: number, py: number, textObj: TextObject, ctx: CanvasRenderingContext2D): boolean => {
    // Ensure font is set correctly for accurate measurement in world space (scaled by zoom)
    const currentFont = `${textObj.fontSize * canvasView.zoom}px ${textObj.fontFamily}`;
    // Temporarily set unscaled font for measurement if needed, or ensure textObj itself uses world units for fontSize if it does not.
    // For simplicity, if textObj.fontSize is in world units, this measure is fine.
    // The rendering part handles scaling. Here we assume textObj properties are world units.
    ctx.font = `${textObj.fontSize}px ${textObj.fontFamily}`; 
    const textMetrics = ctx.measureText(textObj.content);
    const textWidth = textMetrics.width;
    const textHeight = textObj.fontSize; 
    const hitTestY = textObj.y - textHeight * 0.75; // Assumes textObj.y is baseline
    return px >= textObj.x && px <= textObj.x + textWidth && py >= hitTestY && py <= hitTestY + textHeight;
  };

  // Function to get handle positions for a given object (Rectangle or Ellipse)
  const getHandles = (obj: RectangleObject | EllipseObject): Record<HandleType, {x: number, y: number}> | null => {
    if (obj.type === 'rectangle') {
      const { x, y, width, height } = obj;
      return {
        TopLeft: { x, y }, TopMiddle: { x: x + width / 2, y }, TopRight: { x: x + width, y },
        MiddleLeft: { x, y: y + height / 2 }, MiddleRight: { x: x + width, y: y + height / 2 },
        BottomLeft: { x, y: y + height }, BottomMiddle: { x: x + width / 2, y: y + height }, BottomRight: { x: x + width, y: y + height },
      };
    }
    if (obj.type === 'ellipse') {
        const { x, y, radiusX, radiusY } = obj;
        // Bounding box for ellipse handles
        const left = x - radiusX; const top = y - radiusY;
        const right = x + radiusX; const bottom = y + radiusY;
        return {
            TopLeft: { x: left, y: top }, TopMiddle: { x: x, y: top }, TopRight: { x: right, y: top },
            MiddleLeft: { x: left, y: y }, MiddleRight: { x: right, y: y },
            BottomLeft: { x: left, y: bottom }, BottomMiddle: { x: x, y: bottom }, BottomRight: { x: right, y: bottom },
        };
    }
    return null;
  };

  const getCursorForHandle = (handle: HandleType | null): string => {
    if (!handle) return 'default';
    switch (handle) {
      case 'TopLeft': case 'BottomRight': return 'nwse-resize';
      case 'TopRight': case 'BottomLeft': return 'nesw-resize';
      case 'TopMiddle': case 'BottomMiddle': return 'ns-resize';
      case 'MiddleLeft': case 'MiddleRight': return 'ew-resize';
      default: return 'default';
    }
  };

  const handleMouseDown = (event: React.MouseEvent) => {
    const { x: worldMouseX, y: worldMouseY } = getMouseCanvasCoordinates(event);
    const canvas = canvasRef.current;
    let ctx: CanvasRenderingContext2D | null = null;
    if (canvas) {
        ctx = canvas.getContext('2d');
    }

    if (isSpacebarDown || (activeTool === ToolType.SELECT && event.button === 1)) { // Spacebar or Middle mouse for pan
        startPanning(event.clientX, event.clientY, canvasView.panX, canvasView.panY);
        return;
    }

    if (activeTool === ToolType.SELECT) {
      const selectedObj = selectedObjectIds.length === 1 ? objects.find(o => o.id === selectedObjectIds[0]) : undefined;
      
      // Check for handle click first if an object is selected and resizable
      if (selectedObj && (selectedObj.type === 'rectangle' || selectedObj.type === 'ellipse')) {
        const handles = getHandles(selectedObj as RectangleObject | EllipseObject);
        const handleSizeWorld = (HANDLE_SIZE + HANDLE_HIT_AREA_PADDING * 2) / canvasView.zoom; // Handle size in world units
        if (handles) {
          for (const handleType in handles) {
            const handlePos = handles[handleType as HandleType];
            if ( worldMouseX >= handlePos.x - handleSizeWorld / 2 && worldMouseX <= handlePos.x + handleSizeWorld / 2 &&
                 worldMouseY >= handlePos.y - handleSizeWorld / 2 && worldMouseY <= handlePos.y + handleSizeWorld / 2
            ) {
              let snapshot: Partial<RectangleObject | EllipseObject>;
              if (selectedObj.type === 'rectangle') {
                const { id, type, x, y, width, height, opacity, name, fillColor, strokeColor, strokeWidth } = selectedObj as RectangleObject;
                snapshot = { id, type, x, y, width, height, opacity, name, fillColor, strokeColor, strokeWidth };
              } else { // Ellipse
                const { id, type, x, y, radiusX, radiusY, opacity, name, fillColor, strokeColor, strokeWidth } = selectedObj as EllipseObject;
                snapshot = { id, type, x, y, radiusX, radiusY, opacity, name, fillColor, strokeColor, strokeWidth };
              }
              startResizing(selectedObj.id, handleType as HandleType, worldMouseX, worldMouseY, snapshot);
              return;
            }
          }
        }
      }

      // If not clicking a handle, proceed with object selection/drag
      let hitObject: CanvasObject | undefined = undefined;
      if (ctx) {
        for (let i = objects.length - 1; i >= 0; i--) {
            const obj = objects[i];
            let hit = false;
            if (obj.type === 'rectangle') hit = isPointInRect(worldMouseX, worldMouseY, obj as RectangleObject);
            else if (obj.type === 'ellipse') hit = isPointInEllipse(worldMouseX, worldMouseY, obj as EllipseObject);
            else if (obj.type === 'path') hit = isPointOnPath(worldMouseX, worldMouseY, obj as PathObject);
            else if (obj.type === 'text') hit = isPointInText(worldMouseX, worldMouseY, obj as TextObject, ctx);
            
            if (hit) {
              hitObject = obj;
              break; 
            }
        }
      }

      if (hitObject) {
        if (!selectedObjectIds.includes(hitObject.id)) {
          setSelectedObjectIds([hitObject.id]);
        }
        // Now start dragging this object
        startDragging(hitObject.id, worldMouseX, worldMouseY, hitObject.x, hitObject.y);
      } else {
        setSelectedObjectIds([]);
        endDragging(); // Ensure no dragging state if clicked on empty canvas
      }
      return; 
    }
    
    // Reset dragging state if a drawing tool is activated by mousedown
    if (drawingState.isDragging) endDragging();

    if (drawingState.isPanning) endPanning(); // Should not happen if spacebar logic is correct, but as safeguard.

    if (drawingState.isResizing) endResizing(); // End resizing if mouse is down

    if (activeTool === ToolType.TEXT) {
      const newTextData: Omit<TextObject, 'id'> = {
        type: 'text', name: 'Text', x: worldMouseX, y: worldMouseY, content: DEFAULT_TEXT_CONTENT, fontSize: DEFAULT_FONT_SIZE, 
        fontFamily: DEFAULT_FONT_FAMILY, fillColor: DEFAULT_TEXT_COLOR, opacity: 1,
      };
      const newId = addObject(newTextData);
      setSelectedObjectIds([newId]); 
      return; 
    }
    if (activeTool === ToolType.RECTANGLE) {
      const newRectData: Omit<RectangleObject, 'id'> = {
        type: 'rectangle', name: 'Rectangle', x: worldMouseX, y: worldMouseY, width: 0, height: 0, fillColor: '#aabbcc', opacity: 1,
      };
      const newId = addObject(newRectData);
      startDrawing(worldMouseX, worldMouseY, newId);
    } else if (activeTool === ToolType.ELLIPSE) {
      const newEllipseData: Omit<EllipseObject, 'id'> = {
        type: 'ellipse', name: 'Ellipse', x: worldMouseX, y: worldMouseY, radiusX: 0, radiusY: 0, fillColor: '#ccbbaa', opacity: 1,
      };
      const newId = addObject(newEllipseData);
      startDrawing(worldMouseX, worldMouseY, newId);
    } else if (activeTool === ToolType.PEN) {
      const currentPathId = drawingState.currentObjectId;
      const existingPathObject = objects.find(obj => obj.id === currentPathId && obj.type === 'path');
      const existingPath = existingPathObject as PathObject | undefined;
      if (drawingState.isDrawing && existingPath && existingPath.points.length > 0) {
        const firstPoint = existingPath.points[0];
        const distanceToStart = Math.sqrt(distSq(worldMouseX, worldMouseY, firstPoint.x, firstPoint.y));
        if (distanceToStart < PATH_CLOSING_THRESHOLD && existingPath.points.length > 1) {
          const closedPoints = [...existingPath.points, { x: firstPoint.x, y: firstPoint.y }];
          updateObject(currentPathId!, { points: closedPoints } as Partial<PathObject>); 
          endDrawing();
          setPreviewLineEndPoint(null);
        } else {
          const updatedPoints = [...existingPath.points, { x: worldMouseX, y: worldMouseY }];
          updateObject(currentPathId!, { points: updatedPoints } as Partial<PathObject>); 
        }
      } else {
        endDrawing(); 
        const newPathData: Omit<PathObject, 'id'> = {
          type: 'path', name: 'Path', points: [{ x: worldMouseX, y: worldMouseY }], strokeColor: DEFAULT_PATH_STROKE_COLOR, strokeWidth: DEFAULT_PATH_STROKE_WIDTH, opacity: 1, x: worldMouseX, y: worldMouseY,
        };
        const newId = addObject(newPathData);
        startDrawing(worldMouseX, worldMouseY, newId);
      }
    }
  };

  const handleMouseMove = (event: React.MouseEvent) => {
    const { x: worldMouseX, y: worldMouseY } = getMouseCanvasCoordinates(event);
    const screenMouseX = event.clientX;
    const screenMouseY = event.clientY;

    if (drawingState.isPanning) {
        updatePan(screenMouseX, screenMouseY); // updatePan uses screen coordinates to calculate delta from screen startPoint
        return;
    }

    if (drawingState.isResizing && drawingState.currentObjectId && drawingState.startPoint && drawingState.activeHandle && drawingState.resizeObjectSnapshot) {
      const obj = drawingState.resizeObjectSnapshot;
      const { startPoint, activeHandle } = drawingState;
      const dx = worldMouseX - startPoint.x;
      const dy = worldMouseY - startPoint.y;
      let newProps: Partial<RectangleObject | EllipseObject> = {};

      if (obj.type === 'rectangle') {
        let { x = 0, y = 0, width = 0, height = 0 } = obj as Partial<RectangleObject>; 
        switch (activeHandle) {
          case 'TopLeft': x += dx; y += dy; width -= dx; height -= dy; break;
          case 'TopMiddle': y += dy; height -= dy; break;
          case 'TopRight': y += dy; width += dx; height -= dy; break;
          case 'MiddleLeft': x += dx; width -= dx; break;
          case 'MiddleRight': width += dx; break;
          case 'BottomLeft': x += dx; height += dy; width -= dx; break;
          case 'BottomMiddle': height += dy; break;
          case 'BottomRight': width += dx; height += dy; break;
        }
        if (width < 0) { x += width; width *= -1; } // Handle negative width/height by flipping
        if (height < 0) { y += height; height *= -1; }
        newProps = { x, y, width, height };
      } else if (obj.type === 'ellipse') {
        let { x = 0, y = 0, radiusX = 0, radiusY = 0 } = obj as Partial<EllipseObject>; 
        // For ellipses, resizing from corners/edges modifies radii and potentially center
        // This simplified version adjusts radii based on which edge/corner is dragged
        // For a more intuitive feel, one might need to adjust center as well for some handles
        const initialBoundingBox = {
            left: (obj.x ?? 0) - (obj.radiusX ?? 0), top: (obj.y ?? 0) - (obj.radiusY ?? 0),
            right: (obj.x ?? 0) + (obj.radiusX ?? 0), bottom: (obj.y ?? 0) + (obj.radiusY ?? 0),
        };
        let newLeft = initialBoundingBox.left, newTop = initialBoundingBox.top;
        let newRight = initialBoundingBox.right, newBottom = initialBoundingBox.bottom;

        if (activeHandle.includes('Left')) newLeft += dx;
        if (activeHandle.includes('Right')) newRight += dx;
        if (activeHandle.includes('Top')) newTop += dy;
        if (activeHandle.includes('Bottom')) newBottom += dy;
        
        if (activeHandle === 'MiddleLeft' || activeHandle === 'MiddleRight') { /* only x affected */ } 
        else if (activeHandle === 'TopMiddle' || activeHandle === 'BottomMiddle') { /* only y affected */ } 
        else { /* Corner handles affect both */ }

        if (newLeft > newRight) [newLeft, newRight] = [newRight, newLeft];
        if (newTop > newBottom) [newTop, newBottom] = [newBottom, newTop];
        
        newProps = {
            x: (newLeft + newRight) / 2,
            y: (newTop + newBottom) / 2,
            radiusX: (newRight - newLeft) / 2,
            radiusY: (newBottom - newTop) / 2,
        };
      }
      if (Object.keys(newProps).length > 0) updateObject(drawingState.currentObjectId, newProps);
      return; // Prevent other mouse move actions
    }

    // Hovering over handles for cursor change
    let cursorToSet: string | null = null;
    if (activeTool === ToolType.SELECT && selectedObjectIds.length === 1 && !drawingState.isDragging && !drawingState.isResizing) {
        const selectedObj = objects.find(o => o.id === selectedObjectIds[0]);
        if (selectedObj && (selectedObj.type === 'rectangle' || selectedObj.type === 'ellipse')) {
            const handles = getHandles(selectedObj as RectangleObject | EllipseObject);
            const handleSizeWorld = (HANDLE_SIZE + HANDLE_HIT_AREA_PADDING * 2) / canvasView.zoom;
            if (handles) {
                let foundHandle: HandleType | null = null;
                for (const handleType in handles) {
                    const handlePos = handles[handleType as HandleType];
                    if ( worldMouseX >= handlePos.x - handleSizeWorld / 2 && worldMouseX <= handlePos.x + handleSizeWorld / 2 &&
                         worldMouseY >= handlePos.y - handleSizeWorld / 2 && worldMouseY <= handlePos.y + handleSizeWorld / 2 ) {
                        foundHandle = handleType as HandleType;
                        break;
                    }
                }
                setHoveredHandle(foundHandle);
                cursorToSet = getCursorForHandle(foundHandle);
            }
        } else {
            if (hoveredHandle) setHoveredHandle(null); // Clear if selected object is not resizable or no object selected
        }
    } else {
        if (hoveredHandle) setHoveredHandle(null); // Clear if not in select mode or currently dragging/resizing
    }

    if (drawingState.isDragging && drawingState.currentObjectId && drawingState.startPoint) {
        const objToDrag = objects.find(o => o.id === drawingState.currentObjectId);
        if (!objToDrag) {
            endDragging();
            return;
        }

        const deltaX = worldMouseX - drawingState.startPoint.x;
        const deltaY = worldMouseY - drawingState.startPoint.y;

        const newObjX = (drawingState.dragObjectInitialX ?? 0) + deltaX;
        const newObjY = (drawingState.dragObjectInitialY ?? 0) + deltaY;

        if (objToDrag.type === 'path') {
            const path = objToDrag as PathObject;
            const currentStoredPathX = path.x;
            const currentStoredPathY = path.y;
            const pointDeltaX = newObjX - currentStoredPathX;
            const pointDeltaY = newObjY - currentStoredPathY;
            const movedPoints = path.points.map(p => ({
                x: p.x + pointDeltaX,
                y: p.y + pointDeltaY,
            }));

            updateObject(drawingState.currentObjectId, { x: newObjX, y: newObjY, points: movedPoints } as Partial<PathObject>);
        } else {
            updateObject(drawingState.currentObjectId, { x: newObjX, y: newObjY } as Partial<CanvasObject>);
        }
    } else if (activeTool === ToolType.PEN && drawingState.isDrawing && drawingState.currentObjectId) {
      setPreviewLineEndPoint({ x: worldMouseX, y: worldMouseY });
    } else if (drawingState.isDrawing && drawingState.startPoint && drawingState.currentObjectId && activeTool !== ToolType.TEXT) {
      // Shape drawing logic (Rectangle, Ellipse)
      const { startPoint, currentObjectId } = drawingState;
      if (activeTool === ToolType.RECTANGLE) {
        const rectX = Math.min(worldMouseX, startPoint.x);
        const rectY = Math.min(worldMouseY, startPoint.y);
        const rectWidth = Math.abs(worldMouseX - startPoint.x);
        const rectHeight = Math.abs(worldMouseY - startPoint.y);
        updateObject(currentObjectId, { x: rectX, y: rectY, width: rectWidth, height: rectHeight } as Partial<RectangleObject>);
      } else if (activeTool === ToolType.ELLIPSE) {
        const rectX = Math.min(worldMouseX, startPoint.x);
        const rectY = Math.min(worldMouseY, startPoint.y);
        const rectWidth = Math.abs(worldMouseX - startPoint.x);
        const rectHeight = Math.abs(worldMouseY - startPoint.y);
        const centerX = rectX + rectWidth / 2;
        const centerY = rectY + rectHeight / 2;
        const radiusX = rectWidth / 2;
        const radiusY = rectHeight / 2;
        updateObject(currentObjectId, { x: centerX, y: centerY, radiusX, radiusY } as Partial<EllipseObject>);
      }
    } else {
        setPreviewLineEndPoint(null); // Clear pen preview if not applicable
    }
  };

  const handleMouseUp = (event: React.MouseEvent) => {
    if (drawingState.isPanning) { endPanning(); return; }
    if (drawingState.isResizing) { endResizing(); return; } // End resizing on mouse up
    if (drawingState.isDragging) {
        endDragging();
        return; // Important to return after handling drag end
    }
    if (activeTool === ToolType.PEN || activeTool === ToolType.TEXT) {
      return; 
    }
    if (!drawingState.isDrawing || !drawingState.startPoint || !drawingState.currentObjectId) {
      endDrawing();
      return;
    }
    const { x: worldMouseX, y: worldMouseY } = getMouseCanvasCoordinates(event);
    const { startPoint, currentObjectId } = drawingState;
    const dragWidth = Math.abs(worldMouseX - startPoint.x);
    const dragHeight = Math.abs(worldMouseY - startPoint.y);

    if (activeTool === ToolType.RECTANGLE) {
      if (dragWidth < CLICK_DRAG_THRESHOLD && dragHeight < CLICK_DRAG_THRESHOLD) {
        updateObject(currentObjectId, { width: DEFAULT_RECT_WIDTH, height: DEFAULT_RECT_HEIGHT } as Partial<RectangleObject>);
      }
    } else if (activeTool === ToolType.ELLIPSE) {
       if (dragWidth < CLICK_DRAG_THRESHOLD && dragHeight < CLICK_DRAG_THRESHOLD) {
        const centerX = startPoint.x + DEFAULT_ELLIPSE_RADIUS_X;
        const centerY = startPoint.y + DEFAULT_ELLIPSE_RADIUS_Y;
        updateObject(currentObjectId, { x: centerX, y: centerY, radiusX: DEFAULT_ELLIPSE_RADIUS_X, radiusY: DEFAULT_ELLIPSE_RADIUS_Y } as Partial<EllipseObject>);
      }
    }
    endDrawing();
  };

  const handleWheel = (event: React.WheelEvent) => {
    event.preventDefault();
    const canvas = canvasRef.current;
    if (!canvas) return;

    const rect = canvas.getBoundingClientRect();
    const mouseXOnScreen = event.clientX - rect.left; // Mouse X relative to canvas top-left
    const mouseYOnScreen = event.clientY - rect.top; // Mouse Y relative to canvas top-left

    const oldZoom = canvasView.zoom;
    const newZoom = oldZoom * Math.pow(1 - ZOOM_SENSITIVITY, event.deltaY);
    
    // Determine world coordinates of mouse point BEFORE zoom
    const worldMouseXBeforeZoom = (mouseXOnScreen - canvasView.panX) / oldZoom;
    const worldMouseYBeforeZoom = (mouseYOnScreen - canvasView.panY) / oldZoom;

    // Calculate new pan to keep world point under mouse
    const newPanX = mouseXOnScreen - worldMouseXBeforeZoom * newZoom;
    const newPanY = mouseYOnScreen - worldMouseYBeforeZoom * newZoom;

    setCanvasView({ zoom: newZoom, panX: newPanX, panY: newPanY });
  };

  // Drawing logic
  const drawObjects = useCallback((ctx: CanvasRenderingContext2D) => {
    ctx.save(); // Save the default state
    ctx.clearRect(0, 0, ctx.canvas.width / (window.devicePixelRatio||1) , ctx.canvas.height/ (window.devicePixelRatio||1) ); // Clear considering DPR for actual canvas size
    
    ctx.fillStyle = '#f0f0f0';
    ctx.fillRect(0, 0, ctx.canvas.width/ (window.devicePixelRatio||1), ctx.canvas.height/ (window.devicePixelRatio||1));

    // Apply pan and zoom
    ctx.translate(canvasView.panX, canvasView.panY);
    ctx.scale(canvasView.zoom, canvasView.zoom);

    objects.forEach((obj) => {
      ctx.globalAlpha = obj.opacity;
      let isSelected = selectedObjectIds.includes(obj.id);

      switch (obj.type) {
        case 'rectangle':
          const rect = obj as RectangleObject; // Type assertion for clarity
          ctx.fillStyle = rect.fillColor;
          ctx.fillRect(rect.x, rect.y, rect.width, rect.height);
          if (rect.strokeColor && rect.strokeWidth) {
            ctx.strokeStyle = rect.strokeColor;
            ctx.lineWidth = rect.strokeWidth;
            ctx.strokeRect(rect.x, rect.y, rect.width, rect.height);
          }
          if (isSelected) {
            const handles = getHandles(rect);
            if (handles) {
              ctx.strokeStyle = SELECTION_COLOR;
              ctx.lineWidth = SELECTION_LINE_WIDTH / canvasView.zoom;
              // Draw main bounding box for selection
              ctx.strokeRect(rect.x, rect.y, rect.width, rect.height);
              // Draw handles
              ctx.fillStyle = SELECTION_COLOR;
              const handleWorldSize = HANDLE_SIZE / canvasView.zoom;
              Object.values(handles).forEach(pos => {
                ctx.fillRect(pos.x - handleWorldSize / 2, pos.y - handleWorldSize / 2, handleWorldSize, handleWorldSize);
              });
            }
          }
          break;
        case 'ellipse':
          const ellipse = obj as EllipseObject;
          ctx.fillStyle = ellipse.fillColor;
          ctx.beginPath();
          ctx.ellipse(ellipse.x, ellipse.y, ellipse.radiusX, ellipse.radiusY, 0, 0, 2 * Math.PI);
          ctx.fill();
          if (ellipse.strokeColor && ellipse.strokeWidth) {
            ctx.strokeStyle = ellipse.strokeColor;
            ctx.lineWidth = ellipse.strokeWidth;
            ctx.stroke(); // Stroke the current path (the ellipse)
          }
          if (isSelected) {
            const handles = getHandles(ellipse);
            if (handles) {
              ctx.strokeStyle = SELECTION_COLOR;
              ctx.lineWidth = SELECTION_LINE_WIDTH / canvasView.zoom;
              // Draw main bounding box for selection
              ctx.beginPath();
              ctx.ellipse(ellipse.x, ellipse.y, ellipse.radiusX, ellipse.radiusY, 0, 0, 2 * Math.PI);
              ctx.stroke();
              // Draw handles
              ctx.fillStyle = SELECTION_COLOR;
              const handleWorldSize = HANDLE_SIZE / canvasView.zoom;
              Object.values(handles).forEach(pos => {
                ctx.fillRect(pos.x - handleWorldSize / 2, pos.y - handleWorldSize / 2, handleWorldSize, handleWorldSize);
              });
            }
          }
          break;
        case 'path':
          const path = obj as PathObject;
          if (path.points.length > 0) {
            ctx.strokeStyle = path.strokeColor;
            ctx.lineWidth = path.strokeWidth;
            ctx.beginPath();
            ctx.moveTo(path.points[0].x, path.points[0].y);
            for (let i = 1; i < path.points.length; i++) {
              ctx.lineTo(path.points[i].x, path.points[i].y);
            }
            ctx.stroke();
            if (isSelected) {
              // Still highlight points for selected path, as visual cue
              path.points.forEach(p => {
                ctx.fillStyle = SELECTION_COLOR;
                ctx.beginPath();
                ctx.arc(p.x, p.y, (PATH_POINT_SELECTION_RADIUS/2) / canvasView.zoom, 0, 2 * Math.PI); // Smaller radius for points when segment is selectable
                ctx.fill();
              });
            }
          }
          break;
        case 'text':
          const text = obj as TextObject;
          ctx.font = `${text.fontSize}px ${text.fontFamily}`;
          ctx.fillStyle = text.fillColor;
          ctx.textAlign = 'left'; // Default align
          ctx.textBaseline = 'alphabetic'; // Default baseline for y coordinate
          ctx.fillText(text.content, text.x, text.y);
          if (isSelected) {
            // Draw a simple bounding box for selected text
            const textMetrics = ctx.measureText(text.content);
            const approxHeight = text.fontSize; // Approximation
            const textX = text.x;
            const textY = text.y - approxHeight * 0.8; // Approximate top based on baseline
            ctx.strokeStyle = SELECTION_COLOR;
            ctx.lineWidth = SELECTION_LINE_WIDTH / canvasView.zoom;
            ctx.strokeRect(textX - (2/canvasView.zoom), textY - (2/canvasView.zoom), textMetrics.width + (4/canvasView.zoom), approxHeight + (4/canvasView.zoom));
          }
          break;
        // TODO: Add other object types (ellipse, path, text)
      }
      ctx.globalAlpha = 1; // Reset alpha
    });
    
    // Draw preview line for Pen tool
    if (activeTool === ToolType.PEN && drawingState.isDrawing && drawingState.currentObjectId && previewLineEndPoint) {
      const currentPath = objects.find(obj => obj.id === drawingState.currentObjectId && obj.type === 'path') as PathObject | undefined;
      if (currentPath && currentPath.points.length > 0) {
        const lastPoint = currentPath.points[currentPath.points.length - 1];
        ctx.beginPath();
        ctx.moveTo(lastPoint.x, lastPoint.y);
        ctx.lineTo(previewLineEndPoint.x, previewLineEndPoint.y);
        ctx.strokeStyle = DEFAULT_PATH_STROKE_COLOR; // Or a specific preview color
        ctx.lineWidth = DEFAULT_PATH_STROKE_WIDTH / canvasView.zoom; // Adjust preview line for zoom
        ctx.setLineDash([3 / canvasView.zoom, 3 / canvasView.zoom]); // Dashed line for preview
        ctx.stroke();
        ctx.setLineDash([]); // Reset line dash
      }
    }
    ctx.restore(); // Restore to default state
  }, [objects, canvasView, activeTool, drawingState.isDrawing, drawingState.currentObjectId, previewLineEndPoint, selectedObjectIds]);

  // Global event listeners for Spacebar (Panning) and Escape key
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.code === 'Space') {
        event.preventDefault(); // Prevent page scroll
        setIsSpacebarDown(true);
      }
      if (event.key === 'Escape') {
        if (drawingState.isPanning) endPanning();
        else if (drawingState.isResizing) endResizing(); // End resizing on ESC
        else if (drawingState.isDragging) endDragging();
        else if (activeTool === ToolType.PEN && drawingState.isDrawing) {
          endDrawing();
          setPreviewLineEndPoint(null);
        }
      }
    };
    const handleKeyUp = (event: KeyboardEvent) => {
      if (event.code === 'Space') {
        event.preventDefault();
        setIsSpacebarDown(false);
        if (drawingState.isPanning) {
            endPanning(); // If space is released while panning, end pan.
        }
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    window.addEventListener('keyup', handleKeyUp);
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
      window.removeEventListener('keyup', handleKeyUp);
    };
  }, [activeTool, drawingState, endDrawing, endDragging, endPanning, endResizing]);

  // Effect for canvas setup, resize handling, and initial draw
  useEffect(() => {
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (canvas && container) {
      const ctx = canvas.getContext('2d');
      if (ctx) {
        const resizeAndDraw = () => {
          const dpr = window.devicePixelRatio || 1;
          const displayWidth = container.offsetWidth;
          const displayHeight = container.offsetHeight;
          if (canvas.width !== displayWidth * dpr || canvas.height !== displayHeight * dpr) {
            canvas.width = displayWidth * dpr;
            canvas.height = displayHeight * dpr;
            canvas.style.width = displayWidth + 'px';
            canvas.style.height = displayHeight + 'px';
            ctx.scale(dpr, dpr); // Only set scale once after dpr changes resolution
          }
          drawObjects(ctx);
        };
        resizeAndDraw(); 
        window.addEventListener('resize', resizeAndDraw);
        return () => window.removeEventListener('resize', resizeAndDraw);
      }
    }
  }, [drawObjects]); // Only re-run if drawObjects (and its dependencies like objects, canvasView) changes

  // Effect for Pen tool preview (redraws only when preview point changes)
  useEffect(() => {
    if (activeTool === ToolType.PEN && drawingState.isDrawing && previewLineEndPoint) {
        const canvas = canvasRef.current;
        if (canvas) {
            const ctx = canvas.getContext('2d');
            if (ctx) drawObjects(ctx);
        }
    }
  }, [activeTool, drawingState.isDrawing, previewLineEndPoint, drawObjects]);

  // Dynamic cursor style based on current action
  let cursorStyle = 'default';
  if (drawingState.isPanning) cursorStyle = 'grabbing';
  else if (isSpacebarDown) cursorStyle = 'grab';
  else if (hoveredHandle) cursorStyle = getCursorForHandle(hoveredHandle);
  else if (drawingState.isDragging) cursorStyle = 'default';

  return (
    <div 
      ref={containerRef} 
      style={{ 
        flexGrow: 1, 
        backgroundColor: '#e0e0e0', // Fallback, canvas should cover this
        display: 'flex', 
        justifyContent: 'center', 
        alignItems: 'center', 
        overflow: 'hidden', // Ensure canvas doesn't cause scrollbars
        cursor: cursorStyle,
      }}
      onMouseDown={handleMouseDown}
      onMouseMove={handleMouseMove}
      onMouseUp={handleMouseUp}
      onMouseLeave={() => {
        setPreviewLineEndPoint(null);
        setHoveredHandle(null);
        if(isSpacebarDown) setIsSpacebarDown(false); // Reset spacebar if mouse leaves while pressed
        if (drawingState.isPanning) endPanning();
        else if (drawingState.isResizing) endResizing(); // End resizing if mouse leaves canvas
        else if (drawingState.isDragging) endDragging();
        else if (activeTool !== ToolType.PEN && activeTool !== ToolType.TEXT && drawingState.isDrawing) endDrawing();
      }}
      onWheel={handleWheel} // Add wheel event handler for zooming
    >
      <canvas ref={canvasRef} />
    </div>
  );
};

export default CanvasView; 