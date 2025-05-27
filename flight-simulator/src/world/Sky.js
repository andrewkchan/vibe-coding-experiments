import * as THREE from 'three';

export class Sky {
    constructor(scene) {
        this.scene = scene;
        this.clouds = [];
        
        this.createSky();
        this.createClouds();
    }

    createSky() {
        // Create a gradient sky using a large sphere
        const skyGeometry = new THREE.SphereGeometry(1000, 32, 16);
        
        // Flip the sphere inside out
        skyGeometry.scale(-1, 1, 1);
        
        // Create gradient material
        const skyMaterial = new THREE.ShaderMaterial({
            uniforms: {
                topColor: { value: new THREE.Color(0x87CEEB) },
                bottomColor: { value: new THREE.Color(0xFFFFFF) },
                offset: { value: 100 },
                exponent: { value: 0.6 }
            },
            vertexShader: `
                varying vec3 vWorldPosition;
                void main() {
                    vec4 worldPosition = modelMatrix * vec4(position, 1.0);
                    vWorldPosition = worldPosition.xyz;
                    gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
                }
            `,
            fragmentShader: `
                uniform vec3 topColor;
                uniform vec3 bottomColor;
                uniform float offset;
                uniform float exponent;
                varying vec3 vWorldPosition;
                
                void main() {
                    float h = normalize(vWorldPosition + offset).y;
                    h = pow(max(h, 0.0), exponent);
                    gl_FragColor = vec4(mix(bottomColor, topColor, h), 1.0);
                }
            `,
            side: THREE.BackSide
        });
        
        const sky = new THREE.Mesh(skyGeometry, skyMaterial);
        this.scene.add(sky);
    }

    createClouds() {
        // Create cartoon-style clouds
        for (let i = 0; i < 20; i++) {
            const cloud = this.createCloud();
            
            // Random position in the sky
            cloud.position.set(
                (Math.random() - 0.5) * 800,
                100 + Math.random() * 200,
                (Math.random() - 0.5) * 800
            );
            
            // Random scale
            const scale = 0.5 + Math.random() * 1.5;
            cloud.scale.set(scale, scale, scale);
            
            // Store cloud data for animation
            this.clouds.push({
                mesh: cloud,
                speed: 0.5 + Math.random() * 1.5,
                originalY: cloud.position.y
            });
            
            this.scene.add(cloud);
        }
    }

    createCloud() {
        const cloud = new THREE.Group();
        
        const cloudMaterial = new THREE.MeshPhongMaterial({
            color: 0xFFFFFF,
            flatShading: true,
            transparent: true,
            opacity: 0.9
        });
        
        // Create cloud using multiple spheres
        const numSpheres = 4 + Math.floor(Math.random() * 3);
        
        for (let i = 0; i < numSpheres; i++) {
            const radius = 10 + Math.random() * 15;
            const sphereGeometry = new THREE.SphereGeometry(radius, 8, 6);
            const sphere = new THREE.Mesh(sphereGeometry, cloudMaterial);
            
            sphere.position.set(
                (Math.random() - 0.5) * 30,
                (Math.random() - 0.5) * 10,
                (Math.random() - 0.5) * 20
            );
            
            cloud.add(sphere);
        }
        
        return cloud;
    }

    update(elapsedTime) {
        // Animate clouds
        this.clouds.forEach((cloudData) => {
            // Move clouds slowly
            cloudData.mesh.position.x += cloudData.speed * 0.1;
            
            // Wrap around
            if (cloudData.mesh.position.x > 400) {
                cloudData.mesh.position.x = -400;
            }
            
            // Gentle vertical bobbing
            cloudData.mesh.position.y = cloudData.originalY + 
                Math.sin(elapsedTime * 0.5 + cloudData.mesh.position.x * 0.01) * 5;
        });
    }
} 