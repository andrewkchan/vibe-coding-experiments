import React, { useRef, useEffect, useCallback, useState } from 'react';
import { useStore } from '../store/store';
import { ToolType, type CanvasObject, type RectangleObject, type EllipseObject, type PathObject, type TextObject } from '../types/canvas';

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

const CanvasView: React.FC = () => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  // For Pen tool preview line
  const [previewLineEndPoint, setPreviewLineEndPoint] = useState<{x: number, y: number} | null>(null);

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
  } = useStore((state) => state);

  const getMousePosition = (event: React.MouseEvent | MouseEvent): { x: number; y: number } => {
    const canvas = canvasRef.current;
    if (!canvas) return { x: 0, y: 0 };
    const rect = canvas.getBoundingClientRect();
    // TODO: Adjust for canvas pan and zoom later
    return {
      x: event.clientX - rect.left,
      y: event.clientY - rect.top,
    };
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
    ctx.font = `${textObj.fontSize}px ${textObj.fontFamily}`;
    const textMetrics = ctx.measureText(textObj.content);
    // Approximate bounding box. More accurate would involve actualBoundingBoxAscent/Descent.
    const textWidth = textMetrics.width;
    const textHeight = textObj.fontSize; // Approximation

    // Assuming textObj.x, textObj.y is the top-left corner for this hit-test approximation
    // Note: Rendering is typically baseline. For hit-testing, a bounding box is easier.
    // Let's adjust textObj.y to be the top of the text for hit-testing purposes.
    const hitTestY = textObj.y - textHeight * 0.75; // Approximate top based on baseline

    return px >= textObj.x && px <= textObj.x + textWidth && py >= hitTestY && py <= hitTestY + textHeight;
  };

  const handleMouseDown = (event: React.MouseEvent) => {
    const { x, y } = getMousePosition(event);
    const canvas = canvasRef.current;
    let ctx: CanvasRenderingContext2D | null = null;
    if (canvas) {
        ctx = canvas.getContext('2d');
    }

    if (activeTool === ToolType.SELECT) {
      let objectHit = false;
      if (ctx) { // Ensure ctx is available for text hit testing
        for (let i = objects.length - 1; i >= 0; i--) {
            const obj = objects[i];
            let hit = false;
            if (obj.type === 'rectangle') {
            hit = isPointInRect(x, y, obj as RectangleObject);
            } else if (obj.type === 'ellipse') {
            hit = isPointInEllipse(x, y, obj as EllipseObject);
            } else if (obj.type === 'path') {
            hit = isPointOnPath(x, y, obj as PathObject);
            } else if (obj.type === 'text') {
            hit = isPointInText(x, y, obj as TextObject, ctx);
            }

            if (hit) {
            setSelectedObjectIds([obj.id]);
            objectHit = true;
            break; 
            }
        }
      }
      if (!objectHit) {
        setSelectedObjectIds([]);
      }
      return; 
    } else if (activeTool === ToolType.TEXT) {
      const newTextData: Omit<TextObject, 'id'> = {
        type: 'text', x, y, content: DEFAULT_TEXT_CONTENT, fontSize: DEFAULT_FONT_SIZE, 
        fontFamily: DEFAULT_FONT_FAMILY, fillColor: DEFAULT_TEXT_COLOR, opacity: 1,
      };
      const newId = addObject(newTextData);
      setSelectedObjectIds([newId]); 
      return; 
    }
    if (activeTool === ToolType.RECTANGLE) {
      const newRectData: Omit<RectangleObject, 'id'> = {
        type: 'rectangle', x, y, width: 0, height: 0, fillColor: '#aabbcc', opacity: 1,
      };
      const newId = addObject(newRectData);
      startDrawing(x, y, newId);
    } else if (activeTool === ToolType.ELLIPSE) {
      const newEllipseData: Omit<EllipseObject, 'id'> = {
        type: 'ellipse', x, y, radiusX: 0, radiusY: 0, fillColor: '#ccbbaa', opacity: 1,
      };
      const newId = addObject(newEllipseData);
      startDrawing(x, y, newId);
    } else if (activeTool === ToolType.PEN) {
      const currentPathId = drawingState.currentObjectId;
      const existingPathObject = objects.find(obj => obj.id === currentPathId && obj.type === 'path');
      const existingPath = existingPathObject as PathObject | undefined;
      if (drawingState.isDrawing && existingPath && existingPath.points.length > 0) {
        const firstPoint = existingPath.points[0];
        const distanceToStart = Math.sqrt(distSq(x, y, firstPoint.x, firstPoint.y));
        if (distanceToStart < PATH_CLOSING_THRESHOLD && existingPath.points.length > 1) {
          const closedPoints = [...existingPath.points, { x: firstPoint.x, y: firstPoint.y }];
          updateObject(currentPathId!, { points: closedPoints } as Partial<PathObject>); 
          endDrawing();
          setPreviewLineEndPoint(null);
        } else {
          const updatedPoints = [...existingPath.points, { x, y }];
          updateObject(currentPathId!, { points: updatedPoints } as Partial<PathObject>); 
        }
      } else {
        endDrawing(); 
        const newPathData: Omit<PathObject, 'id'> = {
          type: 'path', points: [{ x, y }], strokeColor: DEFAULT_PATH_STROKE_COLOR, strokeWidth: DEFAULT_PATH_STROKE_WIDTH, opacity: 1, x, y,
        };
        const newId = addObject(newPathData);
        startDrawing(x, y, newId);
      }
    }
  };

  const handleMouseMove = (event: React.MouseEvent) => {
    const { x: currentMouseX, y: currentMouseY } = getMousePosition(event);
    
    if (activeTool === ToolType.PEN && drawingState.isDrawing && drawingState.currentObjectId) {
      setPreviewLineEndPoint({ x: currentMouseX, y: currentMouseY });
    } else {
      setPreviewLineEndPoint(null);
    }

    if (!drawingState.isDrawing || !drawingState.startPoint || !drawingState.currentObjectId || activeTool === ToolType.TEXT) return;
    const { startPoint, currentObjectId } = drawingState;

    if (activeTool === ToolType.RECTANGLE) {
      const rectX = Math.min(currentMouseX, startPoint.x);
      const rectY = Math.min(currentMouseY, startPoint.y);
      const rectWidth = Math.abs(currentMouseX - startPoint.x);
      const rectHeight = Math.abs(currentMouseY - startPoint.y);
      updateObject(currentObjectId, {
        x: rectX,
        y: rectY,
        width: rectWidth,
        height: rectHeight,
      } as Partial<RectangleObject>);
    } else if (activeTool === ToolType.ELLIPSE) {
      const rectX = Math.min(currentMouseX, startPoint.x);
      const rectY = Math.min(currentMouseY, startPoint.y);
      const rectWidth = Math.abs(currentMouseX - startPoint.x);
      const rectHeight = Math.abs(currentMouseY - startPoint.y);

      const centerX = rectX + rectWidth / 2;
      const centerY = rectY + rectHeight / 2;
      const radiusX = rectWidth / 2;
      const radiusY = rectHeight / 2;

      updateObject(currentObjectId, {
        x: centerX,
        y: centerY,
        radiusX,
        radiusY,
      } as Partial<EllipseObject>);
    }
  };

  const handleMouseUp = (event: React.MouseEvent) => {
    if (activeTool === ToolType.PEN || activeTool === ToolType.TEXT) {
      return; 
    }
    if (!drawingState.isDrawing || !drawingState.startPoint || !drawingState.currentObjectId) {
      endDrawing();
      return;
    }
    const { x: currentMouseX, y: currentMouseY } = getMousePosition(event);
    const { startPoint, currentObjectId } = drawingState;
    const dragWidth = Math.abs(currentMouseX - startPoint.x);
    const dragHeight = Math.abs(currentMouseY - startPoint.y);

    if (activeTool === ToolType.RECTANGLE) {
      if (dragWidth < CLICK_DRAG_THRESHOLD && dragHeight < CLICK_DRAG_THRESHOLD) {
        updateObject(currentObjectId, {
          width: DEFAULT_RECT_WIDTH,
          height: DEFAULT_RECT_HEIGHT,
        } as Partial<RectangleObject>);
      }
    } else if (activeTool === ToolType.ELLIPSE) {
      if (dragWidth < CLICK_DRAG_THRESHOLD && dragHeight < CLICK_DRAG_THRESHOLD) {
        // Click: create default ellipse, startPoint is top-left of bounding box
        const centerX = startPoint.x + DEFAULT_ELLIPSE_RADIUS_X;
        const centerY = startPoint.y + DEFAULT_ELLIPSE_RADIUS_Y;
        updateObject(currentObjectId, {
          x: centerX,
          y: centerY,
          radiusX: DEFAULT_ELLIPSE_RADIUS_X,
          radiusY: DEFAULT_ELLIPSE_RADIUS_Y,
        } as Partial<EllipseObject>);
      } else {
        // Drag: dimensions already set in mouseMove
        // The x,y (center) and radii are already correctly calculated and updated
      }
    }
    endDrawing();
  };

  // Drawing logic
  const drawObjects = useCallback((ctx: CanvasRenderingContext2D) => {
    ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
    // TODO: Apply pan and zoom from canvasView.zoom, canvasView.panX, canvasView.panY

    // Example background (can be removed or made configurable)
    ctx.fillStyle = '#f0f0f0';
    ctx.fillRect(0, 0, ctx.canvas.width, ctx.canvas.height);

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
            ctx.strokeStyle = SELECTION_COLOR;
            ctx.lineWidth = SELECTION_LINE_WIDTH;
            ctx.strokeRect(rect.x - SELECTION_LINE_WIDTH, rect.y - SELECTION_LINE_WIDTH, rect.width + SELECTION_LINE_WIDTH*2, rect.height + SELECTION_LINE_WIDTH*2);
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
            ctx.strokeStyle = SELECTION_COLOR;
            ctx.lineWidth = SELECTION_LINE_WIDTH;
            ctx.beginPath(); // Start a new path for the selection ellipse
            // Draw ellipse slightly larger for selection outline
            ctx.ellipse(ellipse.x, ellipse.y, ellipse.radiusX + SELECTION_LINE_WIDTH, ellipse.radiusY + SELECTION_LINE_WIDTH, 0, 0, 2 * Math.PI);
            ctx.stroke();
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
                ctx.arc(p.x, p.y, (PATH_POINT_SELECTION_RADIUS/2), 0, 2 * Math.PI); // Smaller radius for points when segment is selectable
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
            ctx.lineWidth = SELECTION_LINE_WIDTH;
            ctx.strokeRect(textX - 2, textY - 2, textMetrics.width + 4, approxHeight + 4);
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
        ctx.lineWidth = DEFAULT_PATH_STROKE_WIDTH;
        ctx.setLineDash([3, 3]); // Dashed line for preview
        ctx.stroke();
        ctx.setLineDash([]); // Reset line dash
      }
    }
  }, [objects, canvasView, activeTool, drawingState.isDrawing, drawingState.currentObjectId, previewLineEndPoint, selectedObjectIds]);

  // Effect for ESC key to end drawing for Pen tool
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        if (activeTool === ToolType.PEN && drawingState.isDrawing) {
          endDrawing();
          setPreviewLineEndPoint(null); // Clear preview line as well
        }
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [activeTool, drawingState.isDrawing, endDrawing]);

  // Effect for canvas setup and resize handling
  useEffect(() => {
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (canvas && container) {
      const ctx = canvas.getContext('2d');
      if (ctx) {
        // Resize canvas to fit container
        const handleResize = () => {
          const dpr = window.devicePixelRatio || 1;
          canvas.width = container.offsetWidth * dpr;
          canvas.height = container.offsetHeight * dpr;
          canvas.style.width = container.offsetWidth + 'px';
          canvas.style.height = container.offsetHeight + 'px';
          ctx.scale(dpr, dpr);
          drawObjects(ctx); // Redraw on resize
        };

        handleResize(); // Initial draw
        window.addEventListener('resize', handleResize);
        return () => window.removeEventListener('resize', handleResize);
      }
    }
  }, [drawObjects]); // Re-run effect if drawObjects changes (due to objects or canvasView)

  // Effect to trigger redraw when previewLineEndPoint changes for pen tool
  useEffect(() => {
    if (activeTool === ToolType.PEN && drawingState.isDrawing) {
        const canvas = canvasRef.current;
        if (canvas) {
            const ctx = canvas.getContext('2d');
            if (ctx) {
                drawObjects(ctx);
            }
        }
    }
  }, [activeTool, drawingState.isDrawing, previewLineEndPoint, drawObjects]);

  return (
    <div 
      ref={containerRef} 
      style={{ 
        flexGrow: 1, 
        backgroundColor: '#e0e0e0', // Fallback, canvas should cover this
        display: 'flex', 
        justifyContent: 'center', 
        alignItems: 'center', 
        overflow: 'hidden' // Ensure canvas doesn't cause scrollbars
      }}
      onMouseDown={handleMouseDown}
      onMouseMove={handleMouseMove}
      onMouseUp={handleMouseUp}
      onMouseLeave={() => {
        setPreviewLineEndPoint(null); // Clear preview line if mouse leaves canvas
        // For shape tools, endDrawing() is appropriate. 
        // For pen tool, leaving the canvas doesn't necessarily mean ending the path.
        if (activeTool !== ToolType.PEN && activeTool !== ToolType.TEXT && drawingState.isDrawing) {
            endDrawing();
        }
      }}
    >
      <canvas ref={canvasRef} />
    </div>
  );
};

export default CanvasView; 