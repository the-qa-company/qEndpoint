export const mergeClasses = (...classes: any[]) => classes
  .filter(c => !!c).join(' ')
