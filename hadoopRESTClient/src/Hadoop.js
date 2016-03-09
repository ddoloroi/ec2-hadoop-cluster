/**
 * Code taken from leafletjs.com
 */
var oldH = window.H,
    H = {};

H.version = '0.0.1';

// define Carousel as a global C variable, saving the original C to restore later if needed

H.noConflict = function () {
    window.H = oldH;
    return this;
};

window.H = H;
