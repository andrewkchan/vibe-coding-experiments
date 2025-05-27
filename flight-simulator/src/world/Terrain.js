import * as THREE from 'three';

export class Terrain {
    constructor(scene) {
        this.scene = scene;
        this.size = 500;
        this.segments = 50;
        
        this.createTerrain();
        this.addTrees();
    }

    createTerrain() {
        // Create terrain geometry with hills
        const geometry = new THREE.PlaneGeometry(
            this.size, 
            this.size, 
            this.segments, 
            this.segments
        );
        
        // Create cartoon-style hills
        const vertices = geometry.attributes.position.array;
        for (let i = 0; i < vertices.length; i += 3) {
            const x = vertices[i];
            const y = vertices[i + 1];
            
            // Create rolling hills with sine waves
            let height = 0;
            height += Math.sin(x * 0.01) * 10;
            height += Math.sin(y * 0.01) * 10;
            height += Math.sin(x * 0.02 + y * 0.02) * 5;
            
            // Add some random variation
            height += (Math.random() - 0.5) * 2;
            
            // Make the center flatter for the airport
            const distFromCenter = Math.sqrt(x * x + y * y);
            if (distFromCenter < 50) {
                height *= 0.1;
            }
            
            vertices[i + 2] = height;
        }
        
        geometry.computeVertexNormals();
        
        // Create a gradient material for cartoon grass
        const material = new THREE.MeshPhongMaterial({
            color: 0x7EC850,
            flatShading: true,
            shininess: 10
        });
        
        const terrain = new THREE.Mesh(geometry, material);
        terrain.rotation.x = -Math.PI / 2;
        terrain.receiveShadow = true;
        
        this.scene.add(terrain);
        
        // Add a water plane for visual interest
        const waterGeometry = new THREE.PlaneGeometry(this.size * 2, this.size * 2);
        const waterMaterial = new THREE.MeshPhongMaterial({
            color: 0x40A4DF,
            transparent: true,
            opacity: 0.8,
            flatShading: true
        });
        
        const water = new THREE.Mesh(waterGeometry, waterMaterial);
        water.rotation.x = -Math.PI / 2;
        water.position.y = -5;
        this.scene.add(water);
    }

    addTrees() {
        const treeGroup = new THREE.Group();
        
        // Create simple cartoon trees
        for (let i = 0; i < 100; i++) {
            const tree = this.createTree();
            
            // Random position avoiding the center (airport area)
            let x, z;
            do {
                x = (Math.random() - 0.5) * this.size * 0.8;
                z = (Math.random() - 0.5) * this.size * 0.8;
            } while (Math.sqrt(x * x + z * z) < 60);
            
            tree.position.set(x, 0, z);
            
            // Random scale for variety
            const scale = 0.8 + Math.random() * 0.4;
            tree.scale.set(scale, scale, scale);
            
            // Random rotation
            tree.rotation.y = Math.random() * Math.PI * 2;
            
            treeGroup.add(tree);
        }
        
        this.scene.add(treeGroup);
    }

    createTree() {
        const tree = new THREE.Group();
        
        // Trunk
        const trunkGeometry = new THREE.CylinderGeometry(1, 1.5, 8, 6);
        const trunkMaterial = new THREE.MeshPhongMaterial({
            color: 0x8B4513,
            flatShading: true
        });
        const trunk = new THREE.Mesh(trunkGeometry, trunkMaterial);
        trunk.position.y = 4;
        trunk.castShadow = true;
        trunk.receiveShadow = true;
        tree.add(trunk);
        
        // Foliage (3 spheres for cartoon look)
        const foliageMaterial = new THREE.MeshPhongMaterial({
            color: 0x228B22,
            flatShading: true
        });
        
        // Bottom sphere
        const sphere1 = new THREE.Mesh(
            new THREE.SphereGeometry(4, 8, 6),
            foliageMaterial
        );
        sphere1.position.y = 8;
        sphere1.castShadow = true;
        tree.add(sphere1);
        
        // Middle sphere
        const sphere2 = new THREE.Mesh(
            new THREE.SphereGeometry(3, 8, 6),
            foliageMaterial
        );
        sphere2.position.y = 11;
        sphere2.castShadow = true;
        tree.add(sphere2);
        
        // Top sphere
        const sphere3 = new THREE.Mesh(
            new THREE.SphereGeometry(2, 8, 6),
            foliageMaterial
        );
        sphere3.position.y = 13;
        sphere3.castShadow = true;
        tree.add(sphere3);
        
        return tree;
    }

    getHeightAt(x, z) {
        // Simple height calculation based on the terrain generation formula
        let height = 0;
        height += Math.sin(x * 0.01) * 10;
        height += Math.sin(z * 0.01) * 10;
        height += Math.sin(x * 0.02 + z * 0.02) * 5;
        
        // Make the center flatter
        const distFromCenter = Math.sqrt(x * x + z * z);
        if (distFromCenter < 50) {
            height *= 0.1;
        }
        
        return height;
    }
} 