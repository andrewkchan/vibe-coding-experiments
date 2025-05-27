Let's build a 3D terrain editor together in the directory `terrain-editor`. The editor should support the following:
1. Run in a web browser.
2. View a terrain-like surface in 3D perspective displayed as a wireframe mesh.
3. Move the camera ("fly") using WASD keys, pan/rotate using the mouse, and move along the global Z axis (up and down) by pressing space or CMD/CTRL (similar to the move, jump, and crouch controls of a first person shooter).
4. Edit terrain using a "fill" tool which allows the user to click on any part of the surface, and add to it by clicking and holding, which results in the heightmap slowly and naturally increasing for a radius around the click. The opposite happens with the "dig" tool.

The map should start out with random terrain which looks reasonably realistic. This could be done via smoothed perlin noise on the heightmap or any other way that you choose. The map is not infinite; it should be bounded by a square. The terrain should not be visible outside of the square, so that the surface looks like a square-shaped heightmap.

Feel free to pick whatever libraries you want. The key parts of the program should be written in Typescript. The project is educational, try to maintain consistency and clean design.

Work only within the `terrain-editor` folder. Do not edit any files outside. Create a plan and write to `PLAN.MD`, and ask for my approval before beginning to code. In the plan, feel free to add any features which you feel are important. Please ask any additional questions and provide suggestions while making the plan.

## Meta

- Cursor agent mode
- Claude 4 Opus
- 150 requests (only a few chat messages though?)