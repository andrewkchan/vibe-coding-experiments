import React from 'react';
import {
  MousePointer,
  Square,
  Circle,
  Edit3, // Pen tool icon
  Type,  // Text tool icon
} from 'react-feather';
import { useStore } from '../store/store';
import { ToolType } from '../types/canvas';

const iconColor = '#333'; // A dark gray color for icons

const Toolbar: React.FC = () => {
  // Select state and actions directly from the store
  const activeTool = useStore((state) => state.activeTool);
  const setActiveTool = useStore((state) => state.setActiveTool);

  const tools = [
    { type: ToolType.SELECT, icon: <MousePointer size={20} color={iconColor} />, name: 'Select' },
    { type: ToolType.RECTANGLE, icon: <Square size={20} color={iconColor} />, name: 'Rectangle' },
    { type: ToolType.ELLIPSE, icon: <Circle size={20} color={iconColor} />, name: 'Ellipse' },
    { type: ToolType.PEN, icon: <Edit3 size={20} color={iconColor} />, name: 'Pen' },
    { type: ToolType.TEXT, icon: <Type size={20} color={iconColor} />, name: 'Text' },
  ];

  const buttonBaseStyle: React.CSSProperties = {
    padding: '8px',
    margin: '0 4px',
    border: '1px solid transparent',
    borderRadius: '4px',
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: 'white', // Default background
  };

  const activeButtonStyle: React.CSSProperties = {
    ...buttonBaseStyle,
    borderColor: '#007bff',
    backgroundColor: '#e7f3ff',
  };

  return (
    <div style={{
      borderBottom: '1px solid #ccc',
      padding: '4px 8px',
      backgroundColor: '#f8f9fa',
      display: 'flex',
      alignItems: 'center',
    }}>
      {tools.map((tool) => (
        <button
          key={tool.type}
          title={tool.name}
          style={activeTool === tool.type ? activeButtonStyle : buttonBaseStyle}
          onClick={() => setActiveTool(tool.type)}
        >
          {tool.icon}
        </button>
      ))}
    </div>
  );
};

export default Toolbar; 