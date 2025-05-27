export class InputManager {
    constructor() {
        this.keys = {};
        this.input = {
            pitch: 0,      // W/S
            roll: 0,       // A/D
            yaw: 0,        // Q/E
            throttle: 0,   // Up/Down arrows
            reset: false,  // Space
            camera: false, // C
            help: false    // H
        };
        
        this.setupEventListeners();
    }

    setupEventListeners() {
        // Keyboard events
        window.addEventListener('keydown', (e) => this.onKeyDown(e));
        window.addEventListener('keyup', (e) => this.onKeyUp(e));
        
        // Prevent default behavior for game keys
        window.addEventListener('keydown', (e) => {
            if (['Space', 'ArrowUp', 'ArrowDown'].includes(e.code)) {
                e.preventDefault();
            }
        });
    }

    onKeyDown(event) {
        this.keys[event.code] = true;
        
        // Handle toggle keys
        if (event.code === 'KeyC' && !event.repeat) {
            this.input.camera = true;
        }
        
        if (event.code === 'KeyH' && !event.repeat) {
            this.input.help = true;
            this.toggleHelp();
        }
    }

    onKeyUp(event) {
        this.keys[event.code] = false;
        
        // Reset toggle states
        if (event.code === 'KeyC') {
            this.input.camera = false;
        }
        if (event.code === 'KeyH') {
            this.input.help = false;
        }
    }

    getInput() {
        // Update continuous inputs
        this.input.pitch = 0;
        this.input.roll = 0;
        this.input.yaw = 0;
        this.input.reset = false;
        
        // Pitch controls (W/S)
        if (this.keys['KeyW']) this.input.pitch = -1;
        if (this.keys['KeyS']) this.input.pitch = 1;
        
        // Roll controls (A/D)
        if (this.keys['KeyA']) this.input.roll = -1;
        if (this.keys['KeyD']) this.input.roll = 1;
        
        // Yaw controls (Q/E)
        if (this.keys['KeyQ']) this.input.yaw = -1;
        if (this.keys['KeyE']) this.input.yaw = 1;
        
        // Throttle controls (Arrow keys)
        if (this.keys['ArrowUp']) {
            this.input.throttle = Math.min(this.input.throttle + 0.02, 1);
        }
        if (this.keys['ArrowDown']) {
            this.input.throttle = Math.max(this.input.throttle - 0.02, 0);
        }
        
        // Reset
        if (this.keys['Space']) {
            this.input.reset = true;
        }
        
        return this.input;
    }

    toggleHelp() {
        const helpElement = document.getElementById('controls-help');
        if (helpElement) {
            helpElement.classList.toggle('hidden');
        }
    }
} 