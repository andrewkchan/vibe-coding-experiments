import React from 'react';
import { useStore } from '../store/store';
import { type CanvasObject, type RectangleObject, type EllipseObject, type PathObject, type TextObject } from '../types/canvas';

const inputStyle: React.CSSProperties = {
  width: 'calc(100% - 10px)',
  padding: '5px',
  marginBottom: '8px',
  border: '1px solid #ccc',
  borderRadius: '3px',
  boxSizing: 'border-box',
};

const textAreaStyle: React.CSSProperties = {
  ...inputStyle,
  minHeight: '60px',
  resize: 'vertical',
};

const labelStyle: React.CSSProperties = {
  display: 'block',
  marginBottom: '2px',
  fontSize: '0.9em',
  color: '#555',
};

const sectionStyle: React.CSSProperties = {
  marginBottom: '15px',
  paddingBottom: '10px',
  borderBottom: '1px solid #eee',
};

const panelTitleStyle: React.CSSProperties = {
  marginTop: 0, 
  marginBottom: '15px', 
  borderBottom: '1px solid #ddd', 
  paddingBottom: '10px',
  color: '#333',
};

const sectionTitleStyle: React.CSSProperties = {
  marginTop: 0, 
  marginBottom: '10px',
  color: '#444',
  fontSize: '1.1em',
};

const PropertiesPanel: React.FC = () => {
  const selectedObjectIds = useStore((state) => state.selectedObjectIds);
  const objects = useStore((state) => state.objects);
  const updateObject = useStore((state) => state.updateObject);

  const selectedObject = React.useMemo(() => 
    selectedObjectIds.length === 1 ? objects.find((obj) => obj.id === selectedObjectIds[0]) : undefined
  , [objects, selectedObjectIds]);

  if (!selectedObject) {
    return (
      <div style={{ width: '250px', borderLeft: '1px solid #ccc', padding: '15px', backgroundColor: '#f9f9f9' }}>
        {selectedObjectIds.length === 0 ? 'No object selected' : 'Multiple objects selected'}
      </div>
    );
  }

  const makeChangeHandler = <O extends CanvasObject, P extends keyof O>(
    objRef: O,
    property: P
  ) => (event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    let newValue: string | number = event.target.value;
    let parsedNumValue: number | undefined = undefined;

    if (typeof objRef[property] === 'number') {
      parsedNumValue = parseFloat(newValue as string);
      if (isNaN(parsedNumValue)) {
        return; 
      }
      newValue = parsedNumValue;
    }

    if (objRef.type === 'path' && (property === 'x' || property === 'y')) {
      const pathObj = objRef as PathObject;
      const currentVal = pathObj[property as 'x' | 'y'];
      const delta = (parsedNumValue !== undefined ? parsedNumValue : parseFloat(newValue as string)) - currentVal;

      if (isNaN(delta)) return;

      const newPoints = pathObj.points.map(p => ({
        x: p.x + (property === 'x' ? delta : 0),
        y: p.y + (property === 'y' ? delta : 0),
      }));
      
      updateObject(objRef.id, { 
        [property]: newValue, 
        points: newPoints 
      } as Partial<PathObject>);
    } else {
      updateObject(objRef.id, { [property]: newValue } as Partial<O>); 
    }
  };

  const renderCommonProperties = (obj: CanvasObject) => (
    <div style={sectionStyle}>
      <h4 style={sectionTitleStyle}>Common Properties</h4>
      <div>
        <label style={labelStyle} htmlFor={`prop-id-${obj.id}`}>ID:</label>
        <input style={{...inputStyle, backgroundColor: '#eee'}} type="text" id={`prop-id-${obj.id}`} value={obj.id} readOnly />
      </div>
      <div>
        <label style={labelStyle} htmlFor={`prop-x-${obj.id}`}>X:</label>
        <input style={inputStyle} type="number" id={`prop-x-${obj.id}`} value={obj.x} onChange={makeChangeHandler(obj, 'x')} />
      </div>
      <div>
        <label style={labelStyle} htmlFor={`prop-y-${obj.id}`}>Y:</label>
        <input style={inputStyle} type="number" id={`prop-y-${obj.id}`} value={obj.y} onChange={makeChangeHandler(obj, 'y')} />
      </div>
      <div>
        <label style={labelStyle} htmlFor={`prop-opacity-${obj.id}`}>Opacity (0-1):</label>
        <input style={inputStyle} type="number" id={`prop-opacity-${obj.id}`} value={obj.opacity} step="0.1" min="0" max="1" onChange={makeChangeHandler(obj, 'opacity')} />
      </div>
    </div>
  );

  const renderSpecificProperties = (obj: CanvasObject) => {
    switch (obj.type) {
      case 'rectangle':
        const rect = obj as RectangleObject;
        return (
          <div style={sectionStyle}>
            <h4 style={sectionTitleStyle}>Rectangle</h4>
            <div>
              <label style={labelStyle} htmlFor={`prop-width-${rect.id}`}>Width:</label>
              <input style={inputStyle} type="number" id={`prop-width-${rect.id}`} value={rect.width} min="0" onChange={makeChangeHandler(rect, 'width')} />
            </div>
            <div>
              <label style={labelStyle} htmlFor={`prop-height-${rect.id}`}>Height:</label>
              <input style={inputStyle} type="number" id={`prop-height-${rect.id}`} value={rect.height} min="0" onChange={makeChangeHandler(rect, 'height')} />
            </div>
            <div>
              <label style={labelStyle} htmlFor={`prop-fill-${rect.id}`}>Fill Color:</label>
              <input style={inputStyle} type="color" id={`prop-fill-${rect.id}`} value={rect.fillColor} onChange={makeChangeHandler(rect, 'fillColor')} />
            </div>
          </div>
        );
      case 'ellipse':
        const ellipse = obj as EllipseObject;
        return (
          <div style={sectionStyle}>
            <h4 style={sectionTitleStyle}>Ellipse</h4>
            <div>
              <label style={labelStyle} htmlFor={`prop-radiusX-${ellipse.id}`}>Radius X:</label>
              <input style={inputStyle} type="number" id={`prop-radiusX-${ellipse.id}`} value={ellipse.radiusX} min="0" onChange={makeChangeHandler(ellipse, 'radiusX')} />
            </div>
            <div>
              <label style={labelStyle} htmlFor={`prop-radiusY-${ellipse.id}`}>Radius Y:</label>
              <input style={inputStyle} type="number" id={`prop-radiusY-${ellipse.id}`} value={ellipse.radiusY} min="0" onChange={makeChangeHandler(ellipse, 'radiusY')} />
            </div>
            <div>
              <label style={labelStyle} htmlFor={`prop-fill-${ellipse.id}`}>Fill Color:</label>
              <input style={inputStyle} type="color" id={`prop-fill-${ellipse.id}`} value={ellipse.fillColor} onChange={makeChangeHandler(ellipse, 'fillColor')} />
            </div>
          </div>
        );
      case 'path':
        const path = obj as PathObject;
        return (
          <div style={sectionStyle}>
            <h4 style={sectionTitleStyle}>Path</h4>
            <div>
              <label style={labelStyle} htmlFor={`prop-strokeColor-${path.id}`}>Stroke Color:</label>
              <input style={inputStyle} type="color" id={`prop-strokeColor-${path.id}`} value={path.strokeColor} onChange={makeChangeHandler(path, 'strokeColor')} />
            </div>
            <div>
              <label style={labelStyle} htmlFor={`prop-strokeWidth-${path.id}`}>Stroke Width:</label>
              <input style={inputStyle} type="number" id={`prop-strokeWidth-${path.id}`} value={path.strokeWidth} min="0" onChange={makeChangeHandler(path, 'strokeWidth')} />
            </div>
          </div>
        );
      case 'text':
        const text = obj as TextObject;
        return (
          <div style={sectionStyle}>
            <h4 style={sectionTitleStyle}>Text</h4>
            <div>
              <label style={labelStyle} htmlFor={`prop-content-${text.id}`}>Content:</label>
              <textarea style={textAreaStyle} id={`prop-content-${text.id}`} value={text.content} onChange={makeChangeHandler(text, 'content')} />
            </div>
            <div>
              <label style={labelStyle} htmlFor={`prop-fontSize-${text.id}`}>Font Size:</label>
              <input style={inputStyle} type="number" id={`prop-fontSize-${text.id}`} value={text.fontSize} min="1" onChange={makeChangeHandler(text, 'fontSize')} />
            </div>
            <div>
              <label style={labelStyle} htmlFor={`prop-fontFamily-${text.id}`}>Font Family:</label>
              <input style={inputStyle} type="text" id={`prop-fontFamily-${text.id}`} value={text.fontFamily} onChange={makeChangeHandler(text, 'fontFamily')} />
            </div>
            <div>
              <label style={labelStyle} htmlFor={`prop-fillColor-text-${text.id}`}>Fill Color:</label>
              <input style={inputStyle} type="color" id={`prop-fillColor-text-${text.id}`} value={text.fillColor} onChange={makeChangeHandler(text, 'fillColor')} />
            </div>
          </div>
        );
      default:
        return <p>Unsupported object type for properties panel.</p>;
    }
  };

  return (
    <div style={{ width: '250px', borderLeft: '1px solid #ccc', padding: '15px', backgroundColor: '#f9f9f9', overflowY: 'auto', height: '100%' }}>
      <h3 style={panelTitleStyle}>Properties</h3>
      {renderCommonProperties(selectedObject!)}
      {renderSpecificProperties(selectedObject!)}
    </div>
  );
};

export default PropertiesPanel; 