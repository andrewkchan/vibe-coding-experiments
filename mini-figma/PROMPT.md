Let's create a project together in the folder `mini-figma`. Based on the name, it will be a web-based 2D vector editor with an infinite canvas.

The over-arching goal is to get the simplest code which will support the most essential 2D vector editing features. Unlike the real Figma, it does not need to support real-time collaboration or complex design features such as blend modes, components, or vector networks. We can start with this basic list of features:
- Infinite canvas with smooth zooming and a toolbar at the top of the screen. The toolbar should display icons. We can use feathericons.
- Selection tool (select paths and shapes). This is the "default" tool that you start out using.
- Pen tool (using the mouse to place one node of a stroked path at a time).
- Rectangle tool (mouse down on the canvas, then drag to create a rectangle, or click once to create a rectangle of some default size).
- Ellipse tool - similar to rectangles but for ellipses.
- Text tool - click to place a text object. The text object can start out with some default text such as "text" so that it takes up some space and is visible onscreen.

Selecting objects using the selection tool should bring up their properties in a panel on the right side of the screen (the "right panel"). Every object must have a position (an X and Y coordinate) and opacity (0.0 to 1.0). Rectangles and ellipses should have width (W) and height (H) properties. Text objects have font size, font family, and text content properties (there are lots of other text properties such as align, justify, text box size, etc., but we will leave those out for now). These properties should all update immediately when the underlying object is directly manipulated on the canvas, and vice versa the underlying object should change when the properties are edited via the right hand panel. Properties should be editable via the appropriate controls.

The left hand panel should contain an ordered list of layers. The layers are ordered such that the top (always visible) comes first and the bottom comes last. 

The UI including these panels and the toolbar which sits on top of the canvas should be in react. Make it look nice; use your imagination.

On the tech side, we also do not need to use WebGL and can start with canvas 2D or another simple API. We definitely don't need WebAssembly; let's use typescript because it is a very popular language which you likely have lots of training data for and which works seamlessly with web APIs. The project should be runnable as a static web page (it can be served via a simple web server).
WRT code style, keep comments to an absolute minimum; the code should be self-documenting as much as possible. It should be modular and extensible, but must not overuse abstractions.

Work only within the `mini-figma` folder. Do not edit any files outside. Create a plan and ask for my approval before beginning to code. In the plan, feel free to add any features which you feel are important, but make sure to honor the overall goals of simplicity and good, clean design. 

## Cursor info

```
59fb4e03-34bd-4216-90ac-a2c64472b977
```