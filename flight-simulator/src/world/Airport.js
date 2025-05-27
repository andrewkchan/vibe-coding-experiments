import * as THREE from 'three';

export class Airport {
    constructor(scene) {
        this.scene = scene;
        
        this.createRunway();
        this.createBuildings();
        this.createWindsock();
    }

    createRunway() {
        // Main runway
        const runwayGeometry = new THREE.BoxGeometry(100, 0.1, 20);
        const runwayMaterial = new THREE.MeshPhongMaterial({
            color: 0x333333,
            flatShading: true
        });
        
        const runway = new THREE.Mesh(runwayGeometry, runwayMaterial);
        runway.position.y = 0.05;
        runway.receiveShadow = true;
        this.scene.add(runway);
        
        // Runway markings
        const markingMaterial = new THREE.MeshBasicMaterial({ color: 0xFFFFFF });
        
        // Center line
        for (let i = -45; i < 45; i += 10) {
            const marking = new THREE.Mesh(
                new THREE.BoxGeometry(6, 0.2, 0.5),
                markingMaterial
            );
            marking.position.set(i, 0.1, 0);
            this.scene.add(marking);
        }
        
        // Threshold markings
        const thresholdGeometry = new THREE.BoxGeometry(2, 0.2, 15);
        const leftThreshold = new THREE.Mesh(thresholdGeometry, markingMaterial);
        leftThreshold.position.set(-48, 0.1, 0);
        this.scene.add(leftThreshold);
        
        const rightThreshold = new THREE.Mesh(thresholdGeometry, markingMaterial);
        rightThreshold.position.set(48, 0.1, 0);
        this.scene.add(rightThreshold);
        
        // Taxiway
        const taxiwayGeometry = new THREE.BoxGeometry(20, 0.1, 50);
        const taxiway = new THREE.Mesh(taxiwayGeometry, runwayMaterial);
        taxiway.position.set(0, 0.05, -35);
        taxiway.receiveShadow = true;
        this.scene.add(taxiway);
    }

    createBuildings() {
        // Control tower
        const tower = new THREE.Group();
        
        // Tower base
        const baseGeometry = new THREE.CylinderGeometry(5, 6, 20, 8);
        const baseMaterial = new THREE.MeshPhongMaterial({
            color: 0xF0E68C,
            flatShading: true
        });
        const base = new THREE.Mesh(baseGeometry, baseMaterial);
        base.position.y = 10;
        base.castShadow = true;
        tower.add(base);
        
        // Tower top
        const topGeometry = new THREE.BoxGeometry(8, 4, 8);
        const topMaterial = new THREE.MeshPhongMaterial({
            color: 0x4169E1,
            flatShading: true
        });
        const top = new THREE.Mesh(topGeometry, topMaterial);
        top.position.y = 22;
        top.castShadow = true;
        tower.add(top);
        
        // Windows
        const windowMaterial = new THREE.MeshPhongMaterial({
            color: 0x87CEEB,
            emissive: 0x87CEEB,
            emissiveIntensity: 0.3
        });
        const windowGeometry = new THREE.BoxGeometry(8.1, 2, 8.1);
        const windows = new THREE.Mesh(windowGeometry, windowMaterial);
        windows.position.y = 22;
        tower.add(windows);
        
        tower.position.set(-30, 0, -50);
        this.scene.add(tower);
        
        // Hangars
        for (let i = 0; i < 3; i++) {
            const hangar = this.createHangar();
            hangar.position.set(30 + i * 20, 0, -50);
            hangar.rotation.y = Math.PI / 2;
            this.scene.add(hangar);
        }
    }

    createHangar() {
        const hangar = new THREE.Group();
        
        // Main structure
        const mainGeometry = new THREE.BoxGeometry(15, 10, 20);
        const mainMaterial = new THREE.MeshPhongMaterial({
            color: 0xC0C0C0,
            flatShading: true
        });
        const main = new THREE.Mesh(mainGeometry, mainMaterial);
        main.position.y = 5;
        main.castShadow = true;
        main.receiveShadow = true;
        hangar.add(main);
        
        // Roof
        const roofGeometry = new THREE.CylinderGeometry(0, 11, 8, 4);
        const roofMaterial = new THREE.MeshPhongMaterial({
            color: 0x8B4513,
            flatShading: true
        });
        const roof = new THREE.Mesh(roofGeometry, roofMaterial);
        roof.position.y = 10;
        roof.rotation.y = Math.PI / 4;
        roof.castShadow = true;
        hangar.add(roof);
        
        // Door
        const doorGeometry = new THREE.BoxGeometry(10, 8, 0.5);
        const doorMaterial = new THREE.MeshPhongMaterial({
            color: 0x696969,
            flatShading: true
        });
        const door = new THREE.Mesh(doorGeometry, doorMaterial);
        door.position.set(0, 4, 10.1);
        hangar.add(door);
        
        return hangar;
    }

    createWindsock() {
        const windsock = new THREE.Group();
        
        // Pole
        const poleGeometry = new THREE.CylinderGeometry(0.2, 0.2, 10);
        const poleMaterial = new THREE.MeshPhongMaterial({
            color: 0xFF0000,
            flatShading: true
        });
        const pole = new THREE.Mesh(poleGeometry, poleMaterial);
        pole.position.y = 5;
        windsock.add(pole);
        
        // Sock
        const sockGeometry = new THREE.ConeGeometry(1, 5, 8);
        const sockMaterial = new THREE.MeshPhongMaterial({
            color: 0xFF6600,
            flatShading: true,
            side: THREE.DoubleSide
        });
        const sock = new THREE.Mesh(sockGeometry, sockMaterial);
        sock.position.y = 10;
        sock.rotation.z = Math.PI / 2;
        windsock.add(sock);
        
        windsock.position.set(60, 0, 0);
        this.scene.add(windsock);
    }
} 