import type { Monaco, monacoTypes } from '@grafana/ui';

import { splLanguage } from './splMonarch';

export const SPL_LANGUAGE_ID = 'essinghigh-splunk-spl';

const registeredMonacoInstances = new WeakSet<Monaco>();

export const registerSplunkLanguage = (monacoInstance: Monaco) => {
  if (registeredMonacoInstances.has(monacoInstance)) {
    return;
  }

  if (!monacoInstance.languages.getLanguages().some((language) => language.id === SPL_LANGUAGE_ID)) {
    monacoInstance.languages.register({ id: SPL_LANGUAGE_ID });
  }

  monacoInstance.languages.setMonarchTokensProvider(
    SPL_LANGUAGE_ID,
    splLanguage as unknown as monacoTypes.languages.IMonarchLanguage
  );
  registeredMonacoInstances.add(monacoInstance);
};
