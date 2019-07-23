#ifndef df026_AsNumpyArrays
#define df026_AsNumpyArrays

// Inject the C++ class CustomObject in the C++ runtime.
class CustomObject {
public:
    int x = 42;
};
// Create a function that returns such an object. This is called to fill the dataframe.
CustomObject fill_object() { return CustomObject(); }

#endif