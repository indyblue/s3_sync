import { h, render, Fragment } from 'preact';
import { useState, useEffect, useRef, useReducer } from 'preact/hooks';
import ih from 'immutability-helper';

export const update = (state, path, value) => ih(state, toObj(path, value, state));

function toObj(arr, value, state) {
  const obj = {}; let o = obj; let oo = state;
  if (typeof arr === 'string') arr = arr.split('.');
  const last = arr.pop();
  while (arr.length) {
    let key = arr.shift();
    if (typeof key === 'function' && Array.isArray(oo)) key = oo.findIndex(key);
    if (!o[key]) o[key] = {};
    if (typeof oo === 'object') oo = oo[key];
    o = o[key];
  }
  o[last] = value;
  return obj;
}

/* eslint-disable object-property-newline */
export const preact = {
  h, render, Fragment,
  useEffect, useState, useRef, useReducer, update,
};
