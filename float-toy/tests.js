document.addEventListener('DOMContentLoaded', () => {
    const testResultsContainer = document.getElementById('test-results');

    const runTests = () => {
        let allTestsPassed = true;

        const assertAlmostEqual = (actual, expected, epsilon = 1e-9) => {
            if (expected === 0) return Math.abs(actual) < epsilon;
            return Math.abs(actual - expected) < epsilon || Math.abs((actual - expected) / expected) < epsilon;
        };
        
        const testCases = [
            // --- E4M3 Tests (non-standard) ---
            { format: 'f8e4m3', name: 'E4M3 Zero', bits: [0, 0,0,0,0, 0,0,0], expected: 0 },
            { format: 'f8e4m3', name: 'E4M3 -Zero', bits: [1, 0,0,0,0, 0,0,0], expected: -0 },
            { format: 'f8e4m3', name: 'E4M3 NaN (S.1111.111)', bits: [0, 1,1,1,1, 1,1,1], expected: NaN },
            { format: 'f8e4m3', name: 'E4M3 Not-Infinity (S.1111.000 is normal)', bits: [0, 1,1,1,1, 0,0,0], expected: 256 },
            { format: 'f8e4m3', name: 'E4M3 Max Normal (S.1111.110)', bits: [0, 1,1,1,1, 1,1,0], expected: 448 },
            { format: 'f8e4m3', name: 'E4M3 Min Normal', bits: [0, 0,0,0,1, 0,0,0], expected: Math.pow(2, -6) },
            { format: 'f8e4m3', name: 'E4M3 Max Subnormal', bits: [0, 0,0,0,0, 1,1,1], expected: 0.875 * Math.pow(2, -6) },
            { format: 'f8e4m3', name: 'E4M3 Min Subnormal', bits: [0, 0,0,0,0, 0,0,1], expected: Math.pow(2, -9) },

            // --- E5M2 Tests (standard IEEE-like) ---
            { format: 'f8e5m2', name: 'E5M2 Zero', bits: [0, 0,0,0,0,0, 0,0], expected: 0 },
            { format: 'f8e5m2', name: 'E5M2 Infinity', bits: [0, 1,1,1,1,1, 0,0], expected: Infinity },
            { format: 'f8e5m2', name: 'E5M2 -Infinity', bits: [1, 1,1,1,1,1, 0,0], expected: -Infinity },
            { format: 'f8e5m2', name: 'E5M2 NaN', bits: [0, 1,1,1,1,1, 0,1], expected: NaN },
            { format: 'f8e5m2', name: 'E5M2 Max Normal', bits: [0, 1,1,1,1,0, 1,1], expected: 57344 },
            { format: 'f8e5m2', name: 'E5M2 Min Normal', bits: [0, 0,0,0,0,1, 0,0], expected: Math.pow(2, -14) },
            { format: 'f8e5m2', name: 'E5M2 Max Subnormal', bits: [0, 0,0,0,0,0, 1,1], expected: 0.75 * Math.pow(2, -14) },
            { format: 'f8e5m2', name: 'E5M2 Min Subnormal', bits: [0, 0,0,0,0,0, 0,1], expected: Math.pow(2, -16) },
        ];

        const resultsHTML = testCases.map(test => {
            const result = calculateValue(test.format, test.bits);
            let pass = false;

            if (isNaN(test.expected)) {
                pass = isNaN(result);
            } else if (!isFinite(test.expected)) {
                pass = result === test.expected;
            } else if (Object.is(test.expected, -0)) {
                pass = Object.is(result, -0);
            } else {
                pass = assertAlmostEqual(result, test.expected);
            }

            if (!pass) {
                allTestsPassed = false;
            }

            return `
                <div class="test-case ${pass ? 'pass' : 'fail'}">
                    <div class="test-name">${test.name}</div>
                    <div class="test-detail">
                        Bits: [${test.bits.join('')}] -> Expected: ${test.expected}, Got: ${result}
                    </div>
                </div>
            `;
        }).join('');
        
        const summary = document.createElement('h2');
        if (allTestsPassed) {
            summary.textContent = 'All Tests Passed!';
            summary.style.color = '#2ecc71';
        } else {
            summary.textContent = 'Some Tests Failed!';
            summary.style.color = '#e74c3c';
        }
        testResultsContainer.prepend(summary);
        testResultsContainer.innerHTML += resultsHTML;
    };

    // Ensure FLOAT_FORMATS is loaded before running tests
    if (typeof calculateValue !== 'undefined') {
        runTests();
    } else {
        console.error("Main script not loaded yet.");
    }
}); 