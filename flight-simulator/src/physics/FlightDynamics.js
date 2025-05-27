import * as THREE from 'three';

export class FlightDynamics {
    constructor(cessna) {
        this.cessna = cessna;
        
        // Arcade-style physics parameters
        this.maxSpeed = 150; // km/h
        this.minSpeed = 20;  // km/h for stall
        this.acceleration = 30;
        this.deceleration = 20;
        
        // Control responsiveness
        this.pitchRate = 1.5;
        this.rollRate = 2.0;
        this.yawRate = 1.0;
        
        // Lift and gravity
        this.gravity = -9.8;
        this.liftFactor = 1.2; // Higher for more arcade feel
        
        // Ground height
        this.groundLevel = 0;
    }

    update(deltaTime, input) {
        if (input.reset) {
            this.cessna.reset();
            return;
        }
        
        // Update throttle
        this.cessna.throttle = input.throttle;
        
        // Calculate forward speed based on throttle
        const targetSpeed = input.throttle * this.maxSpeed;
        const currentSpeed = this.cessna.velocity.length() * 3.6; // Convert to km/h
        
        // Accelerate/decelerate towards target speed
        let newSpeed;
        if (targetSpeed > currentSpeed) {
            newSpeed = Math.min(targetSpeed, currentSpeed + this.acceleration * deltaTime);
        } else {
            newSpeed = Math.max(targetSpeed, currentSpeed - this.deceleration * deltaTime);
        }
        
        // Apply velocity in forward direction
        const forwardDirection = this.cessna.getWorldDirection();
        const speedMs = newSpeed / 3.6; // Convert back to m/s
        this.cessna.velocity.copy(forwardDirection.multiplyScalar(speedMs));
        
        // Apply rotation controls
        this.applyRotationControls(input, deltaTime);
        
        // Apply lift and gravity
        this.applyLiftAndGravity(deltaTime, speedMs);
        
        // Ground collision
        this.handleGroundCollision();
        
        // Update aircraft
        this.cessna.update(deltaTime);
    }

    applyRotationControls(input, deltaTime) {
        // Calculate rotation rates based on speed (more responsive at higher speeds)
        const speedFactor = Math.min(this.cessna.speed / 100, 1);
        const responsiveness = 0.3 + speedFactor * 0.7;
        
        // Get the aircraft's current rotation as a quaternion for proper local rotations
        const currentQuat = new THREE.Quaternion();
        currentQuat.setFromEuler(this.cessna.rotation);
        
        // Create quaternions for each rotation axis in local space
        const pitchQuat = new THREE.Quaternion();
        const rollQuat = new THREE.Quaternion();
        const yawQuat = new THREE.Quaternion();
        
        // Apply pitch (nose up/down) - local Z axis rotation
        if (input.pitch !== 0) {
            const pitchAngle = input.pitch * this.pitchRate * responsiveness * deltaTime;
            pitchQuat.setFromAxisAngle(new THREE.Vector3(0, 0, 1), pitchAngle);
        }
        
        // Apply roll (banking) - local X axis rotation
        if (input.roll !== 0) {
            const rollAngle = input.roll * this.rollRate * responsiveness * deltaTime;
            rollQuat.setFromAxisAngle(new THREE.Vector3(1, 0, 0), rollAngle);
        }
        
        // Apply yaw (rudder) - local Y axis rotation
        if (input.yaw !== 0) {
            const yawAngle = input.yaw * this.yawRate * responsiveness * deltaTime;
            yawQuat.setFromAxisAngle(new THREE.Vector3(0, 1, 0), yawAngle);
        }
        
        // Apply rotations in the correct order: yaw, pitch, roll
        currentQuat.multiply(yawQuat);
        currentQuat.multiply(pitchQuat);
        currentQuat.multiply(rollQuat);
        
        // Convert back to Euler angles
        this.cessna.rotation.setFromQuaternion(currentQuat);
        
        // Apply damping to angular velocities
        if (input.pitch === 0) {
            // Very gentle auto-level pitch - only when close to level
            if (Math.abs(this.cessna.rotation.z) < 0.1) {
                this.cessna.rotation.z *= 0.995;
            }
        }
        
        if (input.roll === 0) {
            // Very gentle auto-level roll - extract local roll angle
            const forward = this.cessna.getWorldDirection();
            const right = this.cessna.getRightVector();
            const localRoll = Math.atan2(right.y, Math.sqrt(right.x * right.x + right.z * right.z));
            
            if (Math.abs(localRoll) < 0.1) {
                // Apply a small correction towards level
                const levelQuat = new THREE.Quaternion();
                levelQuat.setFromAxisAngle(forward, -localRoll * 0.005);
                currentQuat.multiply(levelQuat);
                this.cessna.rotation.setFromQuaternion(currentQuat);
            }
        }
        
        // Update angular velocities for other systems (like propeller animation)
        this.cessna.angularVelocity.set(
            input.roll * this.rollRate * responsiveness,
            input.yaw * this.yawRate * responsiveness,
            input.pitch * this.pitchRate * responsiveness
        );
    }

    applyLiftAndGravity(deltaTime, speedMs) {
        // Simple lift model - more lift at higher speeds
        const speedKmh = speedMs * 3.6;
        const liftRatio = Math.min(speedKmh / this.minSpeed, 1);
        
        // Get the up vector of the aircraft
        const upVector = this.cessna.getUpVector();
        
        // Calculate lift force (arcade style - always some lift if moving)
        let liftForce = 0;
        if (speedKmh > this.minSpeed * 0.5) {
            liftForce = liftRatio * Math.abs(this.gravity) * this.liftFactor;
            
            // More lift when pitched up - using Z rotation since that's our pitch axis
            const pitchFactor = Math.sin(-this.cessna.rotation.z);
            liftForce *= (1 + pitchFactor * 0.5);
        }
        
        // Apply forces
        const verticalVelocity = upVector.y * liftForce + this.gravity;
        this.cessna.velocity.y += verticalVelocity * deltaTime;
        
        // Limit vertical velocity for arcade feel
        this.cessna.velocity.y = THREE.MathUtils.clamp(this.cessna.velocity.y, -50, 50);
        
        // Add some automatic climb with throttle for easier control
        if (this.cessna.throttle > 0.7 && speedKmh > this.minSpeed) {
            this.cessna.velocity.y += (this.cessna.throttle - 0.7) * 10 * deltaTime;
        }
    }

    handleGroundCollision() {
        // Simple ground collision
        if (this.cessna.position.y <= this.groundLevel + 3) {
            this.cessna.position.y = this.groundLevel + 3;
            
            // Bounce effect
            if (this.cessna.velocity.y < 0) {
                this.cessna.velocity.y *= -0.3; // Small bounce
                
                // Reduce speed on ground contact
                this.cessna.velocity.multiplyScalar(0.95);
            }
            
            // Reset rotation on ground
            if (this.cessna.speed < 10) {
                this.cessna.rotation.x *= 0.9;
                this.cessna.rotation.z *= 0.9;
                this.cessna.angularVelocity.multiplyScalar(0.8);
            }
        }
    }
} 