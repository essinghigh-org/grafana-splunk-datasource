import type { Monaco } from '@grafana/ui';

import { registerSplunkLanguage, SPL_LANGUAGE_ID } from '../splMonaco';

const createMonaco = (languageRegistered = false) => {
  const instance = {
    languages: {
      getLanguages: jest.fn(() => (languageRegistered ? [{ id: SPL_LANGUAGE_ID }] : [])),
      register: jest.fn(),
      setMonarchTokensProvider: jest.fn(),
    },
    editor: {
      defineTheme: jest.fn(),
      setTheme: jest.fn(),
    },
  };

  return { instance, monaco: instance as unknown as Monaco };
};

it('registers SPL once without replacing the Grafana editor theme', () => {
  const { instance, monaco } = createMonaco();

  registerSplunkLanguage(monaco);
  registerSplunkLanguage(monaco);

  expect(instance.languages.register).toHaveBeenCalledTimes(1);
  expect(instance.languages.setMonarchTokensProvider).toHaveBeenCalledTimes(1);
  expect(instance.editor.defineTheme).not.toHaveBeenCalled();
  expect(instance.editor.setTheme).not.toHaveBeenCalled();
});

it('initializes each Monaco registry independently', () => {
  const first = createMonaco();
  const second = createMonaco();

  registerSplunkLanguage(first.monaco);
  registerSplunkLanguage(second.monaco);

  expect(first.instance.languages.setMonarchTokensProvider).toHaveBeenCalledTimes(1);
  expect(second.instance.languages.setMonarchTokensProvider).toHaveBeenCalledTimes(1);
});

it('reuses an existing SPL language registration', () => {
  const { instance, monaco } = createMonaco(true);

  registerSplunkLanguage(monaco);

  expect(instance.languages.register).not.toHaveBeenCalled();
  expect(instance.languages.setMonarchTokensProvider).toHaveBeenCalledTimes(1);
});
