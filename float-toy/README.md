# Float Toy - Interactive Floating Point Visualizer

An interactive web application for exploring and understanding various floating-point formats by manipulating individual bits.

## Features

- Interactive bit manipulation for multiple floating-point formats
- Real-time value calculation and component display
- Clear visual distinction between sign, exponent, and mantissa bits
- Support for special values (NaN, Infinity, Zero, Subnormal)
- Responsive design for mobile and desktop

## Supported Formats

1. **float64** (IEEE 754 Double Precision)
   - 1 sign bit, 11 exponent bits, 52 mantissa bits
   - Bias: 1023

2. **float32** (IEEE 754 Single Precision)
   - 1 sign bit, 8 exponent bits, 23 mantissa bits
   - Bias: 127

3. **float16** (IEEE 754 Half Precision)
   - 1 sign bit, 5 exponent bits, 10 mantissa bits
   - Bias: 15

4. **bfloat16** (Brain Floating Point)
   - 1 sign bit, 8 exponent bits, 7 mantissa bits
   - Bias: 127

5. **f8e5m2** (8-bit Floating Point)
   - 1 sign bit, 5 exponent bits, 2 mantissa bits
   - Bias: 15

6. **f8e4m3** (8-bit Floating Point)
   - 1 sign bit, 4 exponent bits, 3 mantissa bits
   - Bias: 7

## Usage

### Running Locally

1. Navigate to the `float-toy` directory
2. Start a local HTTP server:
   ```bash
   # Using Python 3
   python3 -m http.server 8000
   
   # Using Python 2
   python -m SimpleHTTPServer 8000
   
   # Using Node.js (if http-server is installed)
   npx http-server -p 8000
   ```
3. Open your browser and navigate to `http://localhost:8000`

### How to Use

- **Click any bit** to toggle it between 0 and 1
- **Red bits** represent the sign (0 = positive, 1 = negative)
- **Blue bits** represent the exponent
- **Green bits** represent the mantissa/fraction
- The calculated decimal value and components are displayed below each format

### Understanding the Display

Each widget shows:
- **Value**: The decimal representation of the current bit pattern
- **Sign**: The sign bit value and its meaning (+ or -)
- **Exponent**: The raw exponent value and the effective power of 2
- **Mantissa**: The raw mantissa value and its fractional representation
- **Bias**: The bias value used for the exponent

### Special Values

- **Infinity**: All exponent bits set to 1, all mantissa bits set to 0
- **NaN**: All exponent bits set to 1, at least one mantissa bit set to 1
- **Zero**: All bits set to 0 (can be +0 or -0 depending on sign bit)
- **Subnormal**: All exponent bits set to 0, at least one mantissa bit set to 1

## Technical Details

The webapp is built as a static site using:
- HTML5 for structure
- CSS3 for styling with a modern dark theme
- Vanilla JavaScript for interactivity
- No external dependencies required

### Testing

This project includes a test suite to verify the correctness of the floating-point calculations, especially for the non-standard `f8e4m3` format.

To run the tests, open the `float-toy/test.html` file in your browser.

## Browser Compatibility

Works on all modern browsers that support:
- ES6+ JavaScript features
- CSS Grid and Flexbox
- CSS Custom Properties

## License

This educational tool is provided as-is for learning purposes. 