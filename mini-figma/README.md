# Mini-Figma: A Web-Based Vector Editor

## Overview

Mini-Figma is a lightweight, web-based 2D vector editor with an infinite canvas, inspired by Figma. It's designed to provide essential vector editing features with a focus on simplicity and a clean user interface. This project was developed as a pair programming exercise between a human user and an AI assistant (Gemini 2.5 Pro).

The application is built using React, TypeScript, and the HTML5 Canvas 2D API, all bundled with Vite.

## Features Implemented

*   **Infinite Canvas:** Smooth panning and zooming capabilities.
*   **Toolbar:** Located at the top, providing quick access to drawing and selection tools using Feather Icons.
*   **Selection Tool (`Select`):**
    *   Select shapes (Rectangles, Ellipses) and paths by clicking on them.
    *   Select Text objects.
    *   Visual feedback (bounding box or point highlights) for selected objects.
    *   Drag selected objects across the canvas.
*   **Pen Tool (`Pen`):**
    *   Place nodes one at a time to create multi-segment stroked paths.
    *   Live preview line from the last point to the cursor.
    *   Close paths by clicking near the starting node.
    *   Finalize paths by selecting another tool or pressing `ESC`.
*   **Rectangle Tool (`Rectangle`):**
    *   Click and drag to define rectangle dimensions.
    *   Single click to create a default-sized rectangle.
*   **Ellipse Tool (`Ellipse`):**
    *   Click and drag to define the bounding box for an ellipse.
    *   Single click to create a default-sized ellipse.
*   **Text Tool (`Text`):**
    *   Click to place a text object with default content and styling.
*   **Properties Panel (Right Panel):**
    *   Displays properties of the currently selected single object.
    *   Allows editing of common properties: Name, X, Y, Opacity.
    *   Allows editing of shape-specific properties:
        *   **Rectangle:** Width, Height, Fill Color.
        *   **Ellipse:** Radius X, Radius Y, Fill Color.
        *   **Path:** Stroke Color, Stroke Width.
        *   **Text:** Content, Font Size, Font Family, Fill Color.
    *   Changes in the panel immediately update the object on the canvas, and vice-versa for direct manipulations (like dragging).
    *   Moving a Path object via its X/Y properties in the panel correctly offsets all its constituent points.
*   **Layers Panel (Left Panel):**
    *   Displays an ordered list of all objects on the canvas (topmost object at the top of the list).
    *   Shows an icon representing the object type and its user-defined name (or a default name).
    *   Highlights the layer corresponding to the selected object.
    *   Clicking a layer item selects the corresponding object on the canvas.
*   **UI:**
    *   Responsive layout that fills the browser viewport.
    *   Styled for a clean and modern look.
    *   Cursor changes to indicate active tools, panning, and resizing operations.

## How to Install and Run

1.  **Prerequisites:**
    *   Node.js (which includes npm) installed on your system. Recommended LTS version.

2.  **Clone the repository (if applicable) or navigate to the `mini-figma` project directory.**

3.  **Install Dependencies:**
    Open your terminal in the `mini-figma` project root and run:
    ```bash
    npm install
    ```

4.  **Run the Development Server:**
    After installation is complete, run:
    ```bash
    npm run dev
    ```
    This will start the Vite development server, typically on `http://localhost:5173` (the exact port will be shown in the terminal). Open this URL in your web browser to use the application.

## Development Notes

This application was collaboratively developed by a human user and an AI assistant (Gemini 2.5 Pro). The development process involved:

*   The user providing high-level requirements and feature lists.
*   The AI assistant generating a plan, then iteratively writing and refining the TypeScript/React code, CSS, and project configuration files.
*   The AI assistant using a suite of tools to interact with the user's workspace, including:
    *   Creating and editing files (`edit_file`).
    *   Reading files (`read_file`).
    *   Listing directory contents (`list_dir`).
    *   Running terminal commands (`run_terminal_cmd`) for tasks like project initialization and dependency installation.
    *   Searching the codebase (`codebase_search`, `grep_search`).
*   The user providing feedback, debugging information (like console errors or visual discrepancies), and guiding the AI on architectural choices and refinements.

The primary goal was to achieve the simplest code structure that supports the essential 2D vector editing features, emphasizing modularity and clean design while minimizing over-abstraction.

**Disclaimer:** As an AI-generated application, while functional for the implemented features, it may contain bugs, architectural decisions reflective of the AI's current understanding, or areas for optimization. It serves as an example of AI-assisted development.
