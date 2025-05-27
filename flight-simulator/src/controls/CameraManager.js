import * as THREE from 'three';

export class CameraManager {
    constructor(canvas, cessna) {
        this.canvas = canvas;
        this.cessna = cessna;
        
        this.cameras = {
            chase: null,
            cockpit: null,
            tower: null,
            free: null
        };
        
        this.activeCamera = 'chase';
        this.previousCamera = false;
        
        // Camera smoothing
        this.cameraOffset = new THREE.Vector3();
        this.lookAtTarget = new THREE.Vector3();
        
        this.setupCameras();
    }

    setupCameras() {
        const aspect = window.innerWidth / window.innerHeight;
        
        // Chase camera (3rd person)
        this.cameras.chase = new THREE.PerspectiveCamera(60, aspect, 0.1, 3000);
        this.cameras.chase.position.set(-30, 20, 0);
        
        // Cockpit camera (1st person)
        this.cameras.cockpit = new THREE.PerspectiveCamera(80, aspect, 0.1, 3000);
        
        // Tower camera (static view from control tower)
        this.cameras.tower = new THREE.PerspectiveCamera(45, aspect, 0.1, 3000);
        this.cameras.tower.position.set(-30, 30, -50);
        
        // Free camera (orbiting)
        this.cameras.free = new THREE.PerspectiveCamera(60, aspect, 0.1, 3000);
        this.cameras.free.position.set(50, 50, 50);
        
        // Mouse controls for free camera
        this.setupMouseControls();
    }

    setupMouseControls() {
        let mouseDown = false;
        let mouseX = 0;
        let mouseY = 0;
        let cameraAngleH = 0;
        let cameraAngleV = 0.5;
        let cameraDistance = 100;
        
        this.canvas.addEventListener('mousedown', (e) => {
            if (this.activeCamera === 'free') {
                mouseDown = true;
                mouseX = e.clientX;
                mouseY = e.clientY;
            }
        });
        
        this.canvas.addEventListener('mouseup', () => {
            mouseDown = false;
        });
        
        this.canvas.addEventListener('mousemove', (e) => {
            if (mouseDown && this.activeCamera === 'free') {
                const deltaX = e.clientX - mouseX;
                const deltaY = e.clientY - mouseY;
                
                cameraAngleH += deltaX * 0.005;
                cameraAngleV = Math.max(0.1, Math.min(Math.PI - 0.1, cameraAngleV + deltaY * 0.005));
                
                mouseX = e.clientX;
                mouseY = e.clientY;
                
                // Update free camera position
                const x = Math.sin(cameraAngleH) * Math.sin(cameraAngleV) * cameraDistance;
                const y = Math.cos(cameraAngleV) * cameraDistance;
                const z = Math.cos(cameraAngleH) * Math.sin(cameraAngleV) * cameraDistance;
                
                this.cameras.free.position.set(
                    this.cessna.position.x + x,
                    this.cessna.position.y + y,
                    this.cessna.position.z + z
                );
            }
        });
        
        this.canvas.addEventListener('wheel', (e) => {
            if (this.activeCamera === 'free') {
                e.preventDefault();
                cameraDistance = Math.max(30, Math.min(300, cameraDistance + e.deltaY * 0.1));
            }
        });
    }

    update(deltaTime) {
        const input = window.inputManager?.getInput();
        
        // Switch camera on C key press
        if (input && input.camera && !this.previousCamera) {
            this.switchCamera();
        }
        this.previousCamera = input ? input.camera : false;
        
        // Update active camera based on mode
        switch (this.activeCamera) {
            case 'chase':
                this.updateChaseCamera(deltaTime);
                break;
            case 'cockpit':
                this.updateCockpitCamera();
                break;
            case 'tower':
                this.updateTowerCamera();
                break;
            case 'free':
                // Free camera is updated by mouse controls
                this.cameras.free.lookAt(this.cessna.position);
                break;
        }
    }

    updateChaseCamera(deltaTime) {
        const camera = this.cameras.chase;
        const targetOffset = new THREE.Vector3(-30, 15, 0);
        
        // Apply aircraft rotation to offset
        targetOffset.applyEuler(this.cessna.rotation);
        
        // Smooth camera movement - less aggressive smoothing
        const lerpFactor = 1 - Math.pow(0.01, deltaTime);
        this.cameraOffset.lerp(targetOffset, lerpFactor);
        
        // Update camera position
        camera.position.copy(this.cessna.position).add(this.cameraOffset);
        
        // Look ahead of the aircraft
        const lookAhead = this.cessna.getWorldDirection().multiplyScalar(20);
        this.lookAtTarget.lerp(
            this.cessna.position.clone().add(lookAhead),
            lerpFactor * 2 // Faster look-at updates
        );
        
        camera.lookAt(this.lookAtTarget);
    }

    updateCockpitCamera() {
        const camera = this.cameras.cockpit;
        
        // Position camera in cockpit
        const cockpitOffset = new THREE.Vector3(4, 3, 0);
        cockpitOffset.applyEuler(this.cessna.rotation);
        camera.position.copy(this.cessna.position).add(cockpitOffset);
        
        // Look forward
        const lookDirection = this.cessna.getWorldDirection();
        const lookAt = camera.position.clone().add(lookDirection.multiplyScalar(100));
        camera.lookAt(lookAt);
        
        // Apply aircraft roll to camera - using X rotation since that's our roll axis
        camera.rotation.x = this.cessna.rotation.x;
    }

    updateTowerCamera() {
        const camera = this.cameras.tower;
        camera.lookAt(this.cessna.position);
    }

    switchCamera() {
        const modes = ['chase', 'cockpit', 'tower', 'free'];
        const currentIndex = modes.indexOf(this.activeCamera);
        this.activeCamera = modes[(currentIndex + 1) % modes.length];
    }

    getActiveCamera() {
        return this.cameras[this.activeCamera];
    }

    handleResize() {
        const aspect = window.innerWidth / window.innerHeight;
        Object.values(this.cameras).forEach(camera => {
            camera.aspect = aspect;
            camera.updateProjectionMatrix();
        });
    }
} 