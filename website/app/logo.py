import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

dpi = 80
fig = plt.figure(figsize=(4, 1.6),dpi=dpi)

def add_background():
    ax = fig.add_axes([0., 0., 1., 1.], axisbg='black')
    rect = mpatches.Rectangle([0, .86], 0.05, 0.14, ec="none", facecolor='black')
    ax.add_patch(rect)
    rect = mpatches.Rectangle([0.05, .86], 0.05, 0.14, ec="none", facecolor='gray')
    ax.add_patch(rect)
    rect = mpatches.Rectangle([0, 0.72], 0.05, 0.14, ec="none", facecolor='gray')
    ax.add_patch(rect)
    rect = mpatches.Rectangle([.95, 0], 0.05, 0.14, ec="none", facecolor='black')
    ax.add_patch(rect)
    rect = mpatches.Rectangle([.90, 0], 0.05, 0.14, ec="none", facecolor='gray')
    ax.add_patch(rect)
    rect = mpatches.Rectangle([.95, .14], 0.05, 0.14, ec="none", facecolor='gray')
    ax.add_patch(rect)
    ax.set_axis_off()
    return ax

def add_insight_text(ax):
    ax.text(0.52, 0.5, 'INSIGHT', color='black', fontsize=45,
               ha='center', va='center', alpha=1.0, transform=ax.transAxes)


if __name__ == '__main__':
    main_axes = add_background()
    add_insight_text(main_axes)
    plt.show()
