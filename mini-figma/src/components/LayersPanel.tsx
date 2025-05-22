import React from 'react';
import { useStore } from '../store/store';
import { type CanvasObject } from '../types/canvas';
import { Square, Circle, Edit3, Type as TypeIcon, AlertTriangle } from 'react-feather';

const layerItemBaseStyle: React.CSSProperties = {
  padding: '6px 10px',
  borderBottom: '1px solid #e0e0e0',
  cursor: 'pointer',
  fontSize: '0.85em',
  display: 'flex',
  alignItems: 'center',
  userSelect: 'none',
};

const selectedLayerItemStyle: React.CSSProperties = {
  ...layerItemBaseStyle,
  backgroundColor: '#e7f3ff',
  fontWeight: '600',
  borderLeft: '3px solid #007bff',
  paddingLeft: '7px',
};

const iconContainerStyle: React.CSSProperties = {
  marginRight: '8px',
  display: 'flex',
  alignItems: 'center',
  color: '#555',
};

const layerNameStyle: React.CSSProperties = {
  color: '#333',
  whiteSpace: 'nowrap',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  flexGrow: 1,
};

const LayersPanel: React.FC = () => {
  const objects = useStore((state) => state.objects);
  const selectedObjectIds = useStore((state) => state.selectedObjectIds);
  const setSelectedObjectIds = useStore((state) => state.setSelectedObjectIds);

  const getObjectIcon = (objType: CanvasObject['type']) => {
    const size = 16;
    switch (objType) {
      case 'rectangle': return <Square size={size} />;
      case 'ellipse': return <Circle size={size} />;
      case 'path': return <Edit3 size={size} />;
      case 'text': return <TypeIcon size={size} />;
      default: return <AlertTriangle size={size} />;
    }
  };

  return (
    <div style={{ width: '220px', borderRight: '1px solid #ccc', backgroundColor: '#f9f9f9', height: '100%', overflowY: 'auto' }}>
      <h3 
        style={{
          padding: '10px 12px',
          margin: 0, 
          borderBottom: '1px solid #ddd', 
          fontSize: '1em', 
          fontWeight: '600',
          color: '#333', 
          backgroundColor: '#f0f0f0' 
        }}
      >
        Layers
      </h3>
      {objects.length === 0 && (
        <p style={{ padding: '12px', color: '#777', textAlign: 'center', fontSize: '0.9em' }}>No layers yet.</p>
      )}
      {[...objects].reverse().map((obj) => {
        const isSelected = selectedObjectIds.includes(obj.id);
        const displayName = obj.name || `${obj.type.charAt(0).toUpperCase() + obj.type.slice(1)} Object`;

        return (
          <div
            key={obj.id}
            style={isSelected ? selectedLayerItemStyle : layerItemBaseStyle}
            onClick={() => setSelectedObjectIds([obj.id])}
            title={`${displayName} (ID: ${obj.id.substring(0,6)}...)`}
          >
            <span style={iconContainerStyle}>{getObjectIcon(obj.type)}</span>
            <span style={layerNameStyle}>{displayName}</span>
          </div>
        );
      })}
    </div>
  );
};

export default LayersPanel; 