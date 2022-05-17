import { createSlice, PayloadAction } from '@reduxjs/toolkit';

import { NavModelItem } from '@grafana/data';
import config from 'app/core/config';

import { PreferencesService } from '../services/PreferencesService';

const savedItems = ((config.bootData?.navTree as NavModelItem[]) ?? []).find((n) => n.id === 'saved-items');
const savedItemsIds = savedItems?.children?.map((n) => n.id);

export const initialState: NavModelItem[] = ((config.bootData?.navTree ?? []) as NavModelItem[]).map(
  (n: NavModelItem) => ({
    ...n,
    children: n.children?.map((child) => ({
      ...child,
      isSavedItem: savedItemsIds?.includes(child.id),
    })),
  })
);

const navTreeSlice = createSlice({
  name: 'navBarTree',
  initialState,
  reducers: {
    togglePin: (state, action: PayloadAction<{ id: string }>) => {
      const nav = state
        .flatMap((navItem) => (navItem.id === 'saved-items' ? [] : navItem.children))
        .find((navItem) => navItem?.id === action.payload.id);
      if (nav) {
        nav.isSavedItem = !nav.isSavedItem;
        const savedItems = state.find((navItem) => navItem.id === 'saved-items');
        if (savedItems) {
          savedItems.children = nav.isSavedItem
            ? [...(savedItems.children || []), nav]
            : savedItems.children?.filter((navItem) => navItem.id !== action.payload.id);
        }
        // Persist new savedItems
        const service = new PreferencesService('user');
        service.patch({
          navbar: {
            savedItems: savedItems?.children || [],
          },
        });
      }
    },
  },
});

export const { togglePin } = navTreeSlice.actions;
export const navTreeReducer = navTreeSlice.reducer;
