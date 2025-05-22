import Toolbar from './components/Toolbar';
import LayersPanel from './components/LayersPanel';
import CanvasView from './components/CanvasView';
import PropertiesPanel from './components/PropertiesPanel';
import './App.css'; // We'll create this next for basic styling

function App() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
      <Toolbar />
      <div style={{ display: 'flex', flexGrow: 1 }}>
        <LayersPanel />
        <CanvasView />
        <PropertiesPanel />
      </div>
    </div>
  );
}

export default App;
