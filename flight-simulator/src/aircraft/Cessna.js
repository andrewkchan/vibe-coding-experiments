import * as THREE from 'three';

export class Cessna {
    constructor(scene) {
        this.scene = scene;
        this.mesh = null;
        this.propeller = null;
        
        // Aircraft state
        this.position = new THREE.Vector3(0, 50, 0);
        this.velocity = new THREE.Vector3(0, 0, 0);
        this.rotation = new THREE.Euler(0, 0, 0);
        this.angularVelocity = new THREE.Vector3(0, 0, 0);
        
        // Flight parameters
        this.throttle = 0;
        this.speed = 0;
        this.altitude = 50;
        this.heading = 0;
        
        this.createAircraft();
    }

    createAircraft() {
        const aircraftGroup = new THREE.Group();
        
        // Fuselage (body) - cylindrical body along X axis
        const fuselageGeometry = new THREE.CylinderGeometry(1, 1.2, 8, 8);
        const fuselageMaterial = new THREE.MeshPhongMaterial({
            color: 0xFF6B6B,
            shininess: 100,
            flatShading: true
        });
        const fuselage = new THREE.Mesh(fuselageGeometry, fuselageMaterial);
        fuselage.rotation.z = Math.PI / 2; // Rotate to align with X axis
        fuselage.castShadow = true;
        aircraftGroup.add(fuselage);
        
        // Nose cone
        const noseGeometry = new THREE.ConeGeometry(1, 2, 8);
        const noseMaterial = new THREE.MeshPhongMaterial({
            color: 0xFF6B6B,
            shininess: 100,
            flatShading: true
        });
        const nose = new THREE.Mesh(noseGeometry, noseMaterial);
        nose.position.x = 5;
        nose.rotation.z = -Math.PI / 2;
        nose.castShadow = true;
        aircraftGroup.add(nose);
        
        // Cockpit
        const cockpitGeometry = new THREE.BoxGeometry(2, 1.5, 2);
        const cockpitMaterial = new THREE.MeshPhongMaterial({
            color: 0x4ECDC4,
            shininess: 100,
            flatShading: true
        });
        const cockpit = new THREE.Mesh(cockpitGeometry, cockpitMaterial);
        cockpit.position.set(1, 1.2, 0);
        cockpit.castShadow = true;
        aircraftGroup.add(cockpit);
        
        // Windshield
        const windshieldGeometry = new THREE.BoxGeometry(0.8, 0.8, 1.8);
        const windshieldMaterial = new THREE.MeshPhongMaterial({
            color: 0x87CEEB,
            transparent: true,
            opacity: 0.6,
            flatShading: true
        });
        const windshield = new THREE.Mesh(windshieldGeometry, windshieldMaterial);
        windshield.position.set(2, 1.2, 0);
        aircraftGroup.add(windshield);
        
        // Main wings - horizontal
        const wingGeometry = new THREE.BoxGeometry(1, 0.2, 12);
        const wingMaterial = new THREE.MeshPhongMaterial({
            color: 0xFFE66D,
            shininess: 100,
            flatShading: true
        });
        const wings = new THREE.Mesh(wingGeometry, wingMaterial);
        wings.position.set(0, 0, 0);
        wings.castShadow = true;
        aircraftGroup.add(wings);
        
        // Wing tips (rounded)
        const wingTipGeometry = new THREE.SphereGeometry(0.5, 8, 4);
        const leftWingTip = new THREE.Mesh(wingTipGeometry, wingMaterial);
        leftWingTip.position.set(0, 0, 6);
        leftWingTip.scale.set(2, 0.4, 1);
        aircraftGroup.add(leftWingTip);
        
        const rightWingTip = new THREE.Mesh(wingTipGeometry, wingMaterial);
        rightWingTip.position.set(0, 0, -6);
        rightWingTip.scale.set(2, 0.4, 1);
        aircraftGroup.add(rightWingTip);
        
        // Tail fin (vertical stabilizer)
        const tailFinGeometry = new THREE.BoxGeometry(0.1, 3, 2);
        const tailFinMaterial = new THREE.MeshPhongMaterial({
            color: 0xFF6B6B,
            shininess: 100,
            flatShading: true
        });
        const tailFin = new THREE.Mesh(tailFinGeometry, tailFinMaterial);
        tailFin.position.set(-4, 1.5, 0);
        tailFin.castShadow = true;
        aircraftGroup.add(tailFin);
        
        // Horizontal stabilizer
        const stabilizerGeometry = new THREE.BoxGeometry(0.5, 0.1, 4);
        const stabilizer = new THREE.Mesh(stabilizerGeometry, wingMaterial);
        stabilizer.position.set(-4, 0.5, 0);
        stabilizer.castShadow = true;
        aircraftGroup.add(stabilizer);
        
        // Engine cowling
        const engineGeometry = new THREE.CylinderGeometry(1.3, 1.1, 1, 8);
        const engineMaterial = new THREE.MeshPhongMaterial({
            color: 0x666666,
            shininess: 100,
            flatShading: true
        });
        const engine = new THREE.Mesh(engineGeometry, engineMaterial);
        engine.position.x = 4;
        engine.rotation.z = Math.PI / 2;
        engine.castShadow = true;
        aircraftGroup.add(engine);
        
        // Propeller hub
        const hubGeometry = new THREE.ConeGeometry(0.3, 0.5, 6);
        const hubMaterial = new THREE.MeshPhongMaterial({
            color: 0x333333,
            shininess: 100
        });
        const hub = new THREE.Mesh(hubGeometry, hubMaterial);
        hub.position.x = 4.5;
        hub.rotation.z = -Math.PI / 2;
        aircraftGroup.add(hub);
        
        // Propeller
        const propellerGroup = new THREE.Group();
        const bladeGeometry = new THREE.BoxGeometry(0.1, 4, 0.4);
        const bladeMaterial = new THREE.MeshPhongMaterial({
            color: 0x333333,
            shininess: 100,
            flatShading: true
        });
        
        // Create 2 propeller blades for cleaner look
        for (let i = 0; i < 2; i++) {
            const blade = new THREE.Mesh(bladeGeometry, bladeMaterial);
            blade.rotation.x = (i * Math.PI) / 2;
            propellerGroup.add(blade);
        }
        
        propellerGroup.position.x = 4.8;
        this.propeller = propellerGroup;
        aircraftGroup.add(propellerGroup);
        
        // Landing gear
        const gearGeometry = new THREE.CylinderGeometry(0.2, 0.2, 1.5);
        const gearMaterial = new THREE.MeshPhongMaterial({
            color: 0x333333,
            shininess: 100
        });
        
        // Wheels
        const wheelGeometry = new THREE.CylinderGeometry(0.4, 0.4, 0.2, 8);
        const wheelMaterial = new THREE.MeshPhongMaterial({
            color: 0x222222,
            shininess: 100
        });
        
        // Front gear
        const frontGear = new THREE.Mesh(gearGeometry, gearMaterial);
        frontGear.position.set(2, -1.5, 0);
        aircraftGroup.add(frontGear);
        
        const frontWheel = new THREE.Mesh(wheelGeometry, wheelMaterial);
        frontWheel.position.set(2, -2.2, 0);
        frontWheel.rotation.z = Math.PI / 2;
        aircraftGroup.add(frontWheel);
        
        // Rear gears
        const leftGear = new THREE.Mesh(gearGeometry, gearMaterial);
        leftGear.position.set(-1, -1.5, 2.5);
        aircraftGroup.add(leftGear);
        
        const leftWheel = new THREE.Mesh(wheelGeometry, wheelMaterial);
        leftWheel.position.set(-1, -2.2, 2.5);
        leftWheel.rotation.z = Math.PI / 2;
        aircraftGroup.add(leftWheel);
        
        const rightGear = new THREE.Mesh(gearGeometry, gearMaterial);
        rightGear.position.set(-1, -1.5, -2.5);
        aircraftGroup.add(rightGear);
        
        const rightWheel = new THREE.Mesh(wheelGeometry, wheelMaterial);
        rightWheel.position.set(-1, -2.2, -2.5);
        rightWheel.rotation.z = Math.PI / 2;
        aircraftGroup.add(rightWheel);
        
        // Scale the entire aircraft
        aircraftGroup.scale.set(1.5, 1.5, 1.5);
        
        this.mesh = aircraftGroup;
        this.mesh.position.copy(this.position);
        this.scene.add(this.mesh);
    }

    update(deltaTime) {
        // Update position
        this.position.add(this.velocity.clone().multiplyScalar(deltaTime));
        this.mesh.position.copy(this.position);
        
        // Rotation is now handled directly in FlightDynamics using quaternions
        // Just copy the rotation to the mesh
        this.mesh.rotation.copy(this.rotation);
        
        // Update derived values
        this.altitude = this.position.y;
        this.speed = this.velocity.length() * 3.6; // Convert to km/h
        this.heading = THREE.MathUtils.radToDeg(this.rotation.y) % 360;
        
        // Rotate propeller based on throttle
        if (this.propeller && this.throttle > 0) {
            this.propeller.rotation.x += deltaTime * this.throttle * 50;
        }
    }

    reset() {
        this.position.set(0, 50, 0);
        this.velocity.set(0, 0, 0);
        this.rotation.set(0, 0, 0);
        this.angularVelocity.set(0, 0, 0);
        this.throttle = 0;
        
        this.mesh.position.copy(this.position);
        this.mesh.rotation.set(0, 0, 0);
    }

    getWorldDirection() {
        const direction = new THREE.Vector3(1, 0, 0);
        direction.applyEuler(this.rotation);
        return direction;
    }

    getUpVector() {
        const up = new THREE.Vector3(0, 1, 0);
        up.applyEuler(this.rotation);
        return up;
    }

    getRightVector() {
        const right = new THREE.Vector3(0, 0, -1);
        right.applyEuler(this.rotation);
        return right;
    }
} 