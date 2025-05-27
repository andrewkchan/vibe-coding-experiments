export class HUD {
    constructor() {
        this.elements = {
            speed: document.getElementById('speed'),
            altitude: document.getElementById('altitude'),
            throttle: document.getElementById('throttle')
        };
    }

    update(cessna) {
        // Update speed
        if (this.elements.speed) {
            this.elements.speed.textContent = Math.round(cessna.speed);
        }
        
        // Update altitude
        if (this.elements.altitude) {
            this.elements.altitude.textContent = Math.round(cessna.altitude);
        }
        
        // Update throttle
        if (this.elements.throttle) {
            this.elements.throttle.textContent = Math.round(cessna.throttle * 100);
        }
    }
} 