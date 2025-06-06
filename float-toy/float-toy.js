// Float format specifications
const FLOAT_FORMATS = {
    float64: {
        name: 'float64',
        totalBits: 64,
        signBits: 1,
        exponentBits: 11,
        mantissaBits: 52,
        bias: 1023
    },
    float32: {
        name: 'float32',
        totalBits: 32,
        signBits: 1,
        exponentBits: 8,
        mantissaBits: 23,
        bias: 127
    },
    float16: {
        name: 'float16',
        totalBits: 16,
        signBits: 1,
        exponentBits: 5,
        mantissaBits: 10,
        bias: 15
    },
    bfloat16: {
        name: 'bfloat16',
        totalBits: 16,
        signBits: 1,
        exponentBits: 8,
        mantissaBits: 7,
        bias: 127
    },
    f8e5m2: {
        name: 'f8e5m2',
        totalBits: 8,
        signBits: 1,
        exponentBits: 5,
        mantissaBits: 2,
        bias: 15
    },
    f8e4m3: {
        name: 'f8e4m3',
        totalBits: 8,
        signBits: 1,
        exponentBits: 4,
        mantissaBits: 3,
        bias: 7
    }
};

// Store the bit states for each format
const bitStates = {};

// Initialize bit states for each format
Object.keys(FLOAT_FORMATS).forEach(format => {
    bitStates[format] = new Array(FLOAT_FORMATS[format].totalBits).fill(0);
});

// Create bit elements for a format
function createBitElements(format) {
    const spec = FLOAT_FORMATS[format];
    const container = document.getElementById(`${format}-bits`);
    container.innerHTML = '';
    
    // Create wrapper for bits
    const bitsWrapper = document.createElement('div');
    bitsWrapper.className = 'bits-wrapper';
    
    // Create all bits in order
    for (let i = 0; i < spec.totalBits; i++) {
        let type;
        if (i < spec.signBits) {
            type = 'sign';
        } else if (i < spec.signBits + spec.exponentBits) {
            type = 'exponent';
        } else {
            type = 'mantissa';
        }
        
        const bit = createBit(format, i, type);
        bitsWrapper.appendChild(bit);
    }
    
    container.appendChild(bitsWrapper);
    
    // Calculate bit width including gap (18px width + 2px gap)
    const bitWidth = 20;
    
    // Create indices above
    const indicesContainer = document.createElement('div');
    indicesContainer.className = 'bit-indices';
    for (let i = 0; i < spec.totalBits; i++) {
        const indexLabel = document.createElement('div');
        indexLabel.className = 'bit-index';
        indexLabel.textContent = spec.totalBits - 1 - i;
        indicesContainer.appendChild(indexLabel);
    }
    container.appendChild(indicesContainer);
    
    // Create labels underneath
    const labelsContainer = document.createElement('div');
    labelsContainer.className = 'bit-labels';
    
    // Sign label
    if (spec.signBits > 0) {
        const signLabel = document.createElement('div');
        signLabel.className = 'bit-label sign-label';
        signLabel.textContent = 'S';
        signLabel.style.left = '0px';
        signLabel.style.width = `${spec.signBits * bitWidth - 2}px`;
        labelsContainer.appendChild(signLabel);
    }
    
    // Exponent label
    if (spec.exponentBits > 0) {
        const expLabel = document.createElement('div');
        expLabel.className = 'bit-label exponent-label';
        expLabel.textContent = 'EXPONENT';
        expLabel.style.left = `${spec.signBits * bitWidth}px`;
        expLabel.style.width = `${spec.exponentBits * bitWidth - 2}px`;
        labelsContainer.appendChild(expLabel);
    }
    
    // Mantissa label
    if (spec.mantissaBits > 0) {
        const mantissaLabel = document.createElement('div');
        mantissaLabel.className = 'bit-label mantissa-label';
        mantissaLabel.textContent = 'MANTISSA';
        mantissaLabel.style.left = `${(spec.signBits + spec.exponentBits) * bitWidth}px`;
        mantissaLabel.style.width = `${spec.mantissaBits * bitWidth - 2}px`;
        labelsContainer.appendChild(mantissaLabel);
    }
    
    container.appendChild(labelsContainer);
}

// Create a single bit element
function createBit(format, index, type) {
    const bit = document.createElement('div');
    bit.className = `bit ${type}`;
    bit.dataset.format = format;
    bit.dataset.index = index;
    bit.textContent = '0';
    
    bit.addEventListener('click', () => {
        toggleBit(format, index);
    });
    
    return bit;
}

// Toggle a bit
function toggleBit(format, index) {
    bitStates[format][index] = bitStates[format][index] === 0 ? 1 : 0;
    updateDisplay(format);
}

// Update the display for a format
function updateDisplay(format) {
    const spec = FLOAT_FORMATS[format];
    const bits = bitStates[format];
    
    // Update bit visual states
    const container = document.getElementById(`${format}-bits`);
    const bitElements = container.querySelectorAll('.bit');
    bitElements.forEach((bitEl, index) => {
        bitEl.textContent = bits[index];
        if (bits[index] === 1) {
            bitEl.classList.add('active');
        } else {
            bitEl.classList.remove('active');
        }
    });
    
    // Calculate the value
    const value = calculateValue(format, bits);
    
    // Update value display
    const valueDisplay = document.getElementById(`${format}-value`);
    valueDisplay.innerHTML = formatValueDisplay(value, spec, bits);

    const valueInput = document.getElementById(`${spec.name}-input`);
    const handleValueInput = () => {
        const strValue = valueInput.value.trim();
        const numValue = parseFloat(strValue); // parseFloat handles "Infinity", "NaN"

        const newBits = decimalToBits(spec.name, numValue);
        bitStates[spec.name] = newBits;
        updateDisplay(spec.name); // Re-render the widget
    };

    valueInput.addEventListener('blur', handleValueInput);
    valueInput.addEventListener('keydown', e => {
        if (e.key === 'Enter') {
            handleValueInput();
            valueInput.blur();
        }
    });
}

function decimalToBits(format, value) {
    const spec = FLOAT_FORMATS[format];
    const bits = new Array(spec.totalBits).fill(0);

    const signBit = (value < 0 || Object.is(value, -0)) ? 1 : 0;
    bits[0] = signBit;

    const absValue = Math.abs(value);
    const maxExp = (1 << spec.exponentBits) - 1;
    const maxMantissaInt = (1 << spec.mantissaBits) - 1;

    // --- Handle special input values ---
    if (isNaN(value)) {
        bits.fill(1, spec.signBits, spec.signBits + spec.exponentBits); // Exponent all 1s
        if (format === 'f8e4m3') {
            bits.fill(1, spec.signBits + spec.exponentBits); // Mantissa all 1s for the single NaN
        } else {
            bits[spec.totalBits - 1] = 1; // Canonical quiet NaN
        }
        return bits;
    }

    if (absValue === Infinity) {
        if (format === 'f8e4m3') { // No infinity, use max normal
            bits.fill(1, spec.signBits, spec.signBits + spec.exponentBits -1); // E.g., for E4M3 -> 1110
            bits[spec.signBits + spec.exponentBits -1] = 0
            bits.fill(1, spec.signBits + spec.exponentBits); // Mantissa all 1s
            bits[spec.totalBits -1] = 0 // from paper S.1110.110
            return bits;

        }
        bits.fill(1, spec.signBits, spec.signBits + spec.exponentBits); // Exponent all 1s
        return bits;
    }

    if (absValue === 0) {
        return bits; // Sign is already set
    }

    // --- Calculation for Normal and Subnormal ---
    let biasedExp = 0;
    let mantissaInt = 0;
    let unbiasedExp = Math.floor(Math.log2(absValue));
    const minUnbiasedExp = 1 - spec.bias;

    if (unbiasedExp < minUnbiasedExp) { // Subnormal
        biasedExp = 0;
        const scale = Math.pow(2, minUnbiasedExp);
        const mantissaFraction = absValue / scale;
        mantissaInt = Math.round(mantissaFraction * (1 << spec.mantissaBits));
        if (mantissaInt > maxMantissaInt) { // Rounded up to a normal number
            mantissaInt = 0;
            biasedExp = 1;
        }
    } else { // Normal
        biasedExp = unbiasedExp + spec.bias;
        const mantissaFraction = absValue / Math.pow(2, unbiasedExp) - 1.0;
        mantissaInt = Math.round(mantissaFraction * (1 << spec.mantissaBits));
        if (mantissaInt > maxMantissaInt) { // Rounded up to next exponent
            mantissaInt = 0;
            biasedExp++;
        }
    }

    // --- Clamping and Final Assembly ---
    const f8e4m3MaxExp = (format === 'f8e4m3') ? maxExp-1 : maxExp
    if (biasedExp >= f8e4m3MaxExp) {
        if(format === 'f8e4m3' && biasedExp === maxExp && mantissaInt < maxMantissaInt){
            // This is a valid number, not overflow.
        } else if (format === 'f8e4m3') {
            bits.fill(1, spec.signBits, spec.signBits + spec.exponentBits);
            bits[spec.signBits + spec.exponentBits - 1] = 0;
            bits.fill(1, spec.signBits + spec.exponentBits); // Mantissa all 1s
            bits[spec.totalBits -1] = 0 // from paper S.1110.110
            return bits;
        } else {
            bits.fill(1, spec.signBits, spec.signBits + spec.exponentBits);
            bits.fill(0, spec.signBits + spec.exponentBits);
            return bits;
        }
    }


    // Assemble bits
    for (let i = 0; i < spec.exponentBits; i++) {
        if ((biasedExp >> i) & 1) {
            bits[spec.signBits + spec.exponentBits - 1 - i] = 1;
        }
    }
    for (let i = 0; i < spec.mantissaBits; i++) {
        if ((mantissaInt >> i) & 1) {
            bits[spec.totalBits - 1 - i] = 1;
        }
    }

    return bits;
}

// Calculate the decimal value from bits
function calculateValue(format, bits) {
    const spec = FLOAT_FORMATS[format];
    
    // Extract components
    const sign = bits[0];
    let exponent = 0;
    let mantissa = 0;
    
    // Calculate exponent value
    for (let i = 0; i < spec.exponentBits; i++) {
        exponent = (exponent << 1) | bits[spec.signBits + i];
    }
    
    // Calculate mantissa value
    for (let i = 0; i < spec.mantissaBits; i++) {
        mantissa = (mantissa << 1) | bits[spec.signBits + spec.exponentBits + i];
    }
    
    const maxExponent = (1 << spec.exponentBits) - 1;

    // Special handling for f8e4m3 (no infinity, only one NaN pattern)
    if (format === 'f8e4m3') {
        const maxMantissa = (1 << spec.mantissaBits) - 1;
        if (exponent === maxExponent) {
            if (mantissa === maxMantissa) {
                return NaN; // S.1111.111 is NaN
            }
            // Other S.1111.mmm patterns are normal numbers (reclaimed from infinity)
            const mantissaValue = 1 + mantissa / (1 << spec.mantissaBits);
            const value = Math.pow(2, exponent - spec.bias) * mantissaValue;
            return sign === 0 ? value : -value;
        }
    }

    // Standard IEEE 754-like handling for other formats
    if (exponent === maxExponent) {
        // Infinity or NaN
        if (mantissa === 0) {
            return sign === 0 ? Infinity : -Infinity;
        } else {
            return NaN;
        }
    } else if (exponent === 0) {
        // Zero or subnormal
        if (mantissa === 0) {
            return sign === 0 ? 0 : -0;
        } else {
            // Subnormal number
            const mantissaValue = mantissa / (1 << spec.mantissaBits);
            const value = Math.pow(2, 1 - spec.bias) * mantissaValue;
            return sign === 0 ? value : -value;
        }
    } else {
        // Normal number
        const mantissaValue = 1 + mantissa / (1 << spec.mantissaBits);
        const value = Math.pow(2, exponent - spec.bias) * mantissaValue;
        return sign === 0 ? value : -value;
    }
}

// Format the value display
function formatValueDisplay(value, spec, bits) {
    // Extract components for display
    const sign = bits[0];
    let exponent = 0;
    let mantissa = 0;
    
    for (let i = 0; i < spec.exponentBits; i++) {
        exponent = (exponent << 1) | bits[spec.signBits + i];
    }
    
    for (let i = 0; i < spec.mantissaBits; i++) {
        mantissa = (mantissa << 1) | bits[spec.signBits + spec.exponentBits + i];
    }
    
    let displayStr;
    if (isNaN(value)) {
        displayStr = 'NaN';
    } else if (!isFinite(value)) {
        displayStr = `${value > 0 ? '' : '-'}Infinity`;
    } else if (Object.is(value, -0)) {
        displayStr = '-0';
    } else {
        const absValue = Math.abs(value);
        if (absValue > 1e-5 && absValue < 1e6) {
             displayStr = parseFloat(value.toPrecision(15)).toString();
        } else {
             displayStr = value.toExponential(4);
        }
    }
    
    const mantissaFraction = mantissa / (1 << spec.mantissaBits);
    const effectiveExponent = exponent - spec.bias;
    
    return `
        <div class="decimal-value">
            <label for="${spec.name}-input">Value:</label>
            <input type="text" id="${spec.name}-input" class="value-input" value="${displayStr}">
        </div>
        <div class="components">
            <div class="component">
                <span class="component-label">Sign:</span>
                <span class="component-value">${sign} (${sign === 0 ? '+' : '-'})</span>
            </div>
            <div class="component">
                <span class="component-label">Exponent:</span>
                <span class="component-value">${exponent} (2^${effectiveExponent})</span>
            </div>
            <div class="component">
                <span class="component-label">Mantissa:</span>
                <span class="component-value">${mantissa} (${exponent === 0 ? '' : '1.'}${mantissaFraction.toString().substring(2).padEnd(spec.mantissaBits, '0')})</span>
            </div>
            <div class="component">
                <span class="component-label">Bias:</span>
                <span class="component-value">${spec.bias}</span>
            </div>
        </div>
    `;
}

// Initialize all widgets
function initializeWidgets() {
    Object.keys(FLOAT_FORMATS).forEach(format => {
        createBitElements(format);
        updateDisplay(format);
    });
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', initializeWidgets); 